/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "mpiimpl.h"

struct MPIR_Continue {
    MPIR_Request *cont_req;
    MPIX_Continue_cb_function *cb;
    MPI_Status *status_ptr;
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
    int my_idx;
};
typedef struct MPIR_Continue_context MPIR_Continue_context;

#define MPIR_REQUEST_INTERNAL_CONTEXT_PREALLOC 8

MPIR_Op MPIR_Continue_context_direct[MPIR_REQUEST_INTERNAL_CONTEXT_PREALLOC];
MPIR_Object_alloc_t MPIR_Continue_context_mem = { 0, 0, 0, 0, 0, 0, MPIR_INTERNAL,
                                          sizeof(MPIR_Continue_context), MPIR_Continue_context_direct,
                                          MPIR_REQUEST_INTERNAL_CONTEXT_PREALLOC,
                                          NULL, {0}
};

void MPIR_Continue_callback(MPIR_Request *ob_request)
{
    MPIR_Continue_context *context_ptr = ob_request->cb_context;
    MPIR_Continue *continue_ptr = context_ptr->continue_ptr;
    MPIR_Request *cont_req = continue_ptr->cont_req;
    /* Fill the status */
    if (continue_ptr->status_ptr != MPI_STATUS_IGNORE) {
        continue_ptr->status_ptr[context_ptr->my_idx] = ob_request->status;
    }
    MPIR_Handle_obj_free(&MPIR_Continue_context_mem, context_ptr);
    /* Signal the continue callback */
    int incomplete;
    MPIR_cc_decr(&continue_ptr->pending_request_count, &incomplete);
    if (!incomplete) {
        /* All the op requests associated with this continue callback have completed */
        /* Invoke the continue callback */
        continue_ptr->cb(continue_ptr->status_ptr, continue_ptr->cb_data);
        MPIR_Handle_obj_free(&MPIR_Continue_mem, continue_ptr);
        /* Signal the continuation request */
        MPIR_cc_decr(cont_req->cc_ptr, &incomplete);
        if (!incomplete) {
            /* All the continue callbacks associated with this continuation request have completed */
            /* TODO: Fine-tune the thread safety level. */
            MPIR_Request_free_safe(cont_req);
        }
    }
}

int MPIR_Continue_init_impl(MPIR_Request **cont_req_ptr,
                            MPIR_Info *info_ptr)
{
    *cont_req_ptr = MPIR_Request_create(MPIR_REQUEST_KIND__CONTINUE);
    return MPI_SUCCESS;
}

int MPIR_Continue_impl(MPIR_Request *op_request_ptr, int *flag,
                       MPIX_Continue_cb_function *cb, void *cb_data,
                       MPI_Status *status, MPIR_Request *cont_request_ptr)
{
    MPIR_Continue *continue_ptr = (MPIR_Continue *) MPIR_Handle_obj_alloc(&MPIR_Continue_mem);
    continue_ptr->cont_req = cont_request_ptr;
    continue_ptr->cb = cb;
    continue_ptr->status_ptr = status;
    continue_ptr->cb_data = cb_data;
    MPIR_cc_set(&continue_ptr->pending_request_count, 1);
    MPIR_Continue_context *context_ptr = (MPIR_Continue_context *) MPIR_Handle_obj_alloc(&MPIR_Continue_context_mem);
    context_ptr->continue_ptr = continue_ptr;
    context_ptr->my_idx = 0;
    if (!MPIR_Set_callback_safe(op_request_ptr, MPIR_Continue_callback, context_ptr)) {
        /* the request has already completed. */
        MPIR_Handle_obj_free(&MPIR_Continue_context_mem, context_ptr);
        MPIR_Handle_obj_free(&MPIR_Continue_mem, continue_ptr);
        /* TODO: do we return the error code via return value or status? */
        if (status != MPI_STATUS_IGNORE) {
            *status = op_request_ptr->status;
        }
        *flag = 1;
    } else {
        *flag = 0;
    }
    return MPI_SUCCESS;
}

int MPIR_Continueall_impl(int count, MPI_Request op_requests[],
                          int *flag, MPIX_Continue_cb_function *cb,
                          void *cb_data, MPI_Status *statuses,
                          MPIR_Request *cont_request_ptr)
{
    MPIR_Continue *continue_ptr = (MPIR_Continue *) MPIR_Handle_obj_alloc(&MPIR_Continue_mem);
    continue_ptr->cont_req = cont_request_ptr;
    continue_ptr->cb = cb;
    continue_ptr->status_ptr = statuses;
    continue_ptr->cb_data = cb_data;
    MPIR_cc_set(&continue_ptr->pending_request_count, count);
    int completed_request = 0;
    for (int i = 0; i < count; i++) {
        MPIR_Continue_context *context_ptr = (MPIR_Continue_context *) MPIR_Handle_obj_alloc(&MPIR_Continue_context_mem);
        context_ptr->continue_ptr = continue_ptr;
        context_ptr->my_idx = i;
        MPIR_Request *op_request_ptr;
        MPIR_Request_get_ptr(op_requests[i], op_request_ptr);
        if (!MPIR_Set_callback_safe(op_request_ptr, MPIR_Continue_callback, context_ptr)) {
            MPIR_Handle_obj_free(&MPIR_Continue_context_mem, context_ptr);
            ++completed_request;
        }
    }
    if (completed_request == count) {
        /* All requests have been completed. The callback will not be invoked. */
        MPIR_Handle_obj_free(&MPIR_Continue_mem, continue_ptr);
        /* TODO: do we return the error code via return value or status? */
        if (statuses != MPI_STATUSES_IGNORE) {
            for (int i = 0; i < count; i++) {
                MPIR_Request *op_request_ptr;
                MPIR_Request_get_ptr(op_requests[i], op_request_ptr);
                statuses[i] = op_request_ptr->status;
            }
        }
        *flag = 1;
    } else {
        *flag = 0;
    }
    return MPI_SUCCESS;
}
