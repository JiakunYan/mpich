/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "mpiimpl.h"

struct MPIR_Continue {
    MPIR_Request *cont_req;
    MPIX_Continue_cb_function *cb;
    void *cb_data;
    MPIR_cc_t pending_request_count;
};
typedef struct MPIR_Continue MPIR_Continue;

#define MPIR_CONTINUE_PREALLOC 8

MPIR_Continue MPIR_Continue_direct[MPIR_CONTINUE_PREALLOC];
MPIR_Object_alloc_t MPIR_Continue_mem = { 0, 0, 0, 0, 0, 0, MPIR_INTERNAL,
                                        sizeof(MPIR_Continue), MPIR_Continue_direct,
                                        MPIR_CONTINUE_PREALLOC,
                                        NULL, {0}
};

struct MPIR_Continue_context {
    struct MPIR_Continue* continue_ptr;
    MPI_Status *status_ptr;
};
typedef struct MPIR_Continue_context MPIR_Continue_context;

#define MPIR_REQUEST_INTERNAL_CONTEXT_PREALLOC 8

MPIR_Op MPIR_Continue_context_direct[MPIR_REQUEST_INTERNAL_CONTEXT_PREALLOC];
MPIR_Object_alloc_t MPIR_Continue_context_mem = { 0, 0, 0, 0, 0, 0, MPIR_INTERNAL,
                                          sizeof(MPIR_Continue_context), MPIR_Continue_context_direct,
                                          MPIR_REQUEST_INTERNAL_CONTEXT_PREALLOC,
                                          NULL, {0}
};

void MPIR_Continue_callback(MPIR_Request *op_request, void *cb_context)
{
    MPIR_Continue_context *context_ptr = (MPIR_Continue_context *) cb_context;
    MPIR_Continue *continue_ptr = context_ptr->continue_ptr;
    MPIR_Request *cont_req = continue_ptr->cont_req;
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
        /* Invoke the continue callback */
        continue_ptr->cb(MPI_SUCCESS, continue_ptr->cb_data);
        MPIR_Handle_obj_free(&MPIR_Continue_mem, continue_ptr);
        /* Signal the continuation request */
        MPIR_cc_decr(cont_req->cc_ptr, &incomplete);
        if (!incomplete) {
            /* All the continue callbacks associated with this continuation request have completed */
//            MPIR_Invoke_callback_safe(cont_req);
            MPIR_Request_free_safe(cont_req);
        }
    }
}

int MPIR_Persist_continue_start(MPIR_Request * request)
{
//    MPIR_Request_add_ref(request);
//    MPIR_Cont_request_activate(request);
    return MPI_SUCCESS;
}

int MPIR_Continue_init_impl(int flags, int max_poll,
                            MPIR_Info *info_ptr,
                            MPIR_Request **cont_req_ptr)
{
    *cont_req_ptr = MPIR_Request_create(MPIR_REQUEST_KIND__PREQUEST_CONTINUE);
    /* We use cc to track how many continue object has been attached to this continuation request. */
    MPIR_cc_set(&(*cont_req_ptr)->cc, 0);
    return MPI_SUCCESS;
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
        MPIR_Cont_request_activate(cont_request_ptr);
    }
    /* Create the continue object */
    MPIR_Continue *continue_ptr = (MPIR_Continue *) MPIR_Handle_obj_alloc(&MPIR_Continue_mem);
    continue_ptr->cont_req = cont_request_ptr;
    continue_ptr->cb = cb;
    continue_ptr->cb_data = cb_data;
    MPIR_cc_set(&continue_ptr->pending_request_count, count);
    for (int i = 0; i < count; i++) {
        MPIR_Continue_context *context_ptr = (MPIR_Continue_context *) MPIR_Handle_obj_alloc(&MPIR_Continue_context_mem);
        context_ptr->continue_ptr = continue_ptr;
        MPIR_Assert(MPI_STATUS_IGNORE == MPI_STATUSES_IGNORE);
        if (array_of_statuses != MPI_STATUS_IGNORE) {
            context_ptr->status_ptr = &array_of_statuses[i];
        } else {
            context_ptr->status_ptr = MPI_STATUS_IGNORE;
        }
        if (!MPIR_Set_callback_safe(request_ptrs[i], MPIR_Continue_callback, context_ptr)) {
            /* the request has already completed. */
            MPIR_Continue_callback(request_ptrs[i], context_ptr);
        }
    }
    return MPI_SUCCESS;
}
