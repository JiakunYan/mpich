/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#ifndef MPIR_ATOMIC_FLAG_H_INCLUDED
#define MPIR_ATOMIC_FLAG_H_INCLUDED

#include "mpi.h"
#include "mpichconf.h"

#if MPICH_THREAD_LEVEL == MPI_THREAD_MULTIPLE && \
    MPICH_THREAD_GRANULARITY == MPICH_THREAD_GRANULARITY__VCI

typedef MPL_atomic_int_t MPIR_atomic_flag_t;

static inline void MPIR_atomic_flag_set(MPIR_atomic_flag_t * flag_ptr, bool val)
{
    MPL_atomic_relaxed_store_int(flag_ptr, val);
}

static inline bool MPIR_atomic_flag_get(MPIR_atomic_flag_t * flag_ptr)
{
    return MPL_atomic_relaxed_load_int(flag_ptr);
}

static inline bool MPIR_atomic_flag_swap(MPIR_atomic_flag_t * flag_ptr, bool val)
{
    return MPL_atomic_swap_int(flag_ptr, val);
}

#else

typedef bool MPIR_atomic_flag_t;

static inline void MPIR_atomic_flag_set(MPIR_atomic_flag_t * flag_ptr, bool val)
{
    *flag_ptr = val;
}

static inline bool MPIR_atomic_flag_get(MPIR_atomic_flag_t * flag_ptr)
{
    return *flag_ptr;
}

static inline bool MPIR_atomic_flag_swap(MPIR_atomic_flag_t * flag_ptr, bool val)
{
    bool ret = *flag_ptr;
    *flag_ptr = val;
    return ret;
}

#endif

#endif /* MPIR_ATOMIC_FLAG_H_INCLUDED */
