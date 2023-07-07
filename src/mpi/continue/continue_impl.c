/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "mpiimpl.h"

/* Continue object: a wrapper to a continue callback */
struct MPIR_Continue {
    MPIR_Request *cont_req;
    MPIX_Continue_cb_function *cb;
    void *cb_data;
    MPIR_cc_t pending_request_count;
    struct MPIR_Continue *next;
    bool is_immediate;
};
typedef struct MPIR_Continue MPIR_Continue;

#define MPIR_CONTINUE_PREALLOC 8
MPIR_Continue MPIR_Continue_direct[MPIR_CONTINUE_PREALLOC];
MPIR_Object_alloc_t MPIR_Continue_mem = { 0, 0, 0, 0, 0, 0, MPIR_INTERNAL,
                                        sizeof(MPIR_Continue), MPIR_Continue_direct,
                                        MPIR_CONTINUE_PREALLOC,
                                        NULL, {0}
};

/* Continue context object: carrying data for each op request */
struct MPIR_Continue_context {
    struct MPIR_Continue* continue_ptr;
    MPI_Status *status_ptr;
    /* Used by the on-hold list */
    struct MPIR_Continue_context *next;
    MPIR_Request *op_request;
};
typedef struct MPIR_Continue_context MPIR_Continue_context;

#define MPIR_REQUEST_INTERNAL_CONTEXT_PREALLOC 8
MPIR_Continue_context MPIR_Continue_context_direct[MPIR_REQUEST_INTERNAL_CONTEXT_PREALLOC];
MPIR_Object_alloc_t MPIR_Continue_context_mem = { 0, 0, 0, 0, 0, 0, MPIR_INTERNAL,
                                          sizeof(MPIR_Continue_context), MPIR_Continue_context_direct,
                                          MPIR_REQUEST_INTERNAL_CONTEXT_PREALLOC,
                                          NULL, {0}
};

struct {
    struct MPIR_Continue *head, *tail;
} g_deferred_cont_list = {NULL, NULL};

void complete_op_request(MPIR_Request *op_request, void *cb_context, bool defer_complete);
void MPIR_Continue_callback(MPIR_Request *op_request, void *cb_context);
void attach_continue_context(MPIR_Continue_context *context_ptr, bool defer_complete);

int MPIR_Continue_init_impl(int flags, int max_poll,
                            MPIR_Info *info_ptr,
                            MPIR_Request **cont_req_ptr)
{
    MPIR_Request *cont_req = MPIR_Request_create(MPIR_REQUEST_KIND__CONTINUE);
    /* We use cc to track how many continue object has been attached to this continuation request. */
    MPIR_cc_set(&cont_req->cc, 0);
    /* Initialize the on-hold context list */
    cont_req->u.cont.cont_context_on_hold_list.head = NULL;
    cont_req->u.cont.cont_context_on_hold_list.tail = NULL;
    /* Initialize the poll-only continue list */
    cont_req->u.cont.ready_poll_only_cont_list.head = NULL;
    cont_req->u.cont.ready_poll_only_cont_list.tail = NULL;
    cont_req->u.cont.is_pool_only = flags & MPIX_CONT_POLL_ONLY;
    cont_req->u.cont.max_poll = max_poll;
    *cont_req_ptr = cont_req;
    return MPI_SUCCESS;
}

int MPIR_Continue_start(MPIR_Request * cont_request_ptr)
{
    MPIR_Cont_request_activate(cont_request_ptr);
    /* Attach those on-hold continue context */
    while (cont_request_ptr->u.cont.cont_context_on_hold_list.head) {
        MPIR_Continue_context *context_ptr = cont_request_ptr->u.cont.cont_context_on_hold_list.head;
        LL_DELETE(cont_request_ptr->u.cont.cont_context_on_hold_list.head,
                  cont_request_ptr->u.cont.cont_context_on_hold_list.tail,
                  context_ptr);
        attach_continue_context(context_ptr, false);
    }
    return MPI_SUCCESS;
}

void attach_continue_context(MPIR_Continue_context *context_ptr, bool defer_complete) {
    /* Attach the continue context to the op request */
    if (!MPIR_Register_callback(context_ptr->op_request, MPIR_Continue_callback, context_ptr, false)) {
        /* the request has already completed. */
        complete_op_request(context_ptr->op_request, context_ptr, defer_complete);
    }
}

int MPIR_Continue_impl(MPIR_Request *op_request_ptr,
                       MPIX_Continue_cb_function *cb, void *cb_data,
                       int flags, MPI_Status *status,
                       MPIR_Request *cont_request_ptr)
{
    return MPIR_Continueall_impl(1, &op_request_ptr, cb, cb_data, flags, status, cont_request_ptr);
}

int MPIR_Continueall_impl(int count, MPIR_Request *request_ptrs[],
                          MPIX_Continue_cb_function *cb, void *cb_data, int flags,
                          MPI_Status *array_of_statuses, MPIR_Request *cont_request_ptr)
{
    /* Add one continue to the continuation request */
    int was_incompleted;
    MPIR_cc_incr(cont_request_ptr->cc_ptr, &was_incompleted);
    if (!was_incompleted) {
        MPIR_Request_add_ref(cont_request_ptr);
    }
    /* Set various condition variables */
    bool cont_request_activated = MPIR_Cont_request_is_active(cont_request_ptr);
    bool defer_complete = flags & MPIX_CONT_DEFER_COMPLETE;
    /* Create the continue object for every continue callback */
    MPIR_Continue *continue_ptr = (MPIR_Continue *) MPIR_Handle_obj_alloc(&MPIR_Continue_mem);
    continue_ptr->cont_req = cont_request_ptr;
    continue_ptr->cb = cb;
    continue_ptr->cb_data = cb_data;
    MPIR_cc_set(&continue_ptr->pending_request_count, count);
    if (!cont_request_ptr->u.cont.is_pool_only && flags & MPIX_CONT_IMMEDIATE)
        continue_ptr->is_immediate = true;
    else
        continue_ptr->is_immediate = false;
        
    for (int i = 0; i < count; i++) {
        /* Create the continue context object for every op request */
        MPIR_Continue_context *context_ptr = (MPIR_Continue_context *) MPIR_Handle_obj_alloc(&MPIR_Continue_context_mem);
        context_ptr->continue_ptr = continue_ptr;
        MPIR_Assert(MPI_STATUS_IGNORE == MPI_STATUSES_IGNORE);
        if (array_of_statuses != MPI_STATUS_IGNORE) {
            context_ptr->status_ptr = &array_of_statuses[i];
        } else {
            context_ptr->status_ptr = MPI_STATUS_IGNORE;
        }
        context_ptr->op_request = request_ptrs[i];
        /* attach the continue context to op request */
        if (cont_request_activated) {
            attach_continue_context(context_ptr, defer_complete);
        } else {
            /* The continuation request is inactive. Do not attach yet. */
            LL_APPEND(cont_request_ptr->u.cont.cont_context_on_hold_list.head,
                      cont_request_ptr->u.cont.cont_context_on_hold_list.tail,
                      context_ptr);
        }
    }
    return MPI_SUCCESS;
}

void execute_continue(MPIR_Continue *continue_ptr)
{
    MPIR_Request *cont_req_ptr = continue_ptr->cont_req;
    /* Invoke the continue callback */
    continue_ptr->cb(MPI_SUCCESS, continue_ptr->cb_data);
    MPIR_Handle_obj_free(&MPIR_Continue_mem, continue_ptr);
    /* Signal the continuation request */
    int incomplete;
    MPIR_cc_decr(cont_req_ptr->cc_ptr, &incomplete);
    if (!incomplete) {
        /* All the continue callbacks associated with this continuation request have completed */
        MPIR_Request_free_safe(cont_req_ptr);
    }
}

void complete_op_request(MPIR_Request *op_request, void *cb_context, bool defer_complete)
{
    MPIR_Continue_context *context_ptr = (MPIR_Continue_context *) cb_context;
    MPIR_Continue *continue_ptr = context_ptr->continue_ptr;
    /* Complete this operation request */
    int rc = MPIR_Request_completion_processing(
            op_request, context_ptr->status_ptr);
    if (context_ptr->status_ptr != MPI_STATUS_IGNORE)
        context_ptr->status_ptr->MPI_ERROR = rc;
    if (!MPIR_Request_is_persistent(op_request)) {
        MPIR_Request_free(op_request);
    }
    MPIR_Handle_obj_free(&MPIR_Continue_context_mem, context_ptr);
    /* Signal the continue callback */
    int incomplete;
    MPIR_cc_decr(&continue_ptr->pending_request_count, &incomplete);
    if (!incomplete) {
        /* All the op requests associated with this continue callback have completed */
        MPIR_Request *cont_req_ptr = continue_ptr->cont_req;
        if (continue_ptr->is_immediate && !defer_complete) {
            execute_continue(continue_ptr);
        } else if (cont_req_ptr->u.cont.is_pool_only) {
            // Pool-only continuation request
            // Push to the continuation request local ready list
            LL_APPEND(cont_req_ptr->u.cont.ready_poll_only_cont_list.head,
                      cont_req_ptr->u.cont.ready_poll_only_cont_list.tail,
                      continue_ptr);
        } else {
            // General-purpose continuation request
            // Push to the global ready list
            LL_APPEND(g_deferred_cont_list.head,
                      g_deferred_cont_list.tail,
                      continue_ptr);
        }
    }

}

void MPIR_Continue_callback(MPIR_Request *op_request, void *cb_context)
{
    complete_op_request(op_request, cb_context, false);
}

int MPIR_Continue_progress_request(MPIR_Request *cont_request_ptr)
{
    MPIR_Assert(cont_request_ptr && cont_request_ptr->kind == MPIR_REQUEST_KIND__CONTINUE);
    int count = 0;
    while (cont_request_ptr->u.cont.ready_poll_only_cont_list.head) {
        MPIR_Continue *continue_ptr = cont_request_ptr->u.cont.ready_poll_only_cont_list.head;
        LL_DELETE(cont_request_ptr->u.cont.ready_poll_only_cont_list.head,
                  cont_request_ptr->u.cont.ready_poll_only_cont_list.tail,
                  continue_ptr);
        execute_continue(continue_ptr);
        if (cont_request_ptr->u.cont.max_poll && ++count >= cont_request_ptr->u.cont.max_poll)
            break;
    }
    return count;
}

void MPIR_Continue_progress(MPIR_Request *request)
{
    int count = 0;
    int max_poll = 0; // By default we poll unlimited time
    if (request && request->kind == MPIR_REQUEST_KIND__CONTINUE) {
        // This is a continuation request
        count += MPIR_Continue_progress_request(request);
        max_poll = request->u.cont.max_poll;
    }
    // make progress on the global list
    while ((!max_poll || count < max_poll) && g_deferred_cont_list.head) {
        MPIR_Continue *continue_ptr = g_deferred_cont_list.head;
        LL_DELETE(g_deferred_cont_list.head,
                  g_deferred_cont_list.tail,
                  continue_ptr);
        execute_continue(continue_ptr);
        ++count;
    }
}