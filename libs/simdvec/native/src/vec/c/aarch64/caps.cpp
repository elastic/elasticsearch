/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

 // This file contains implementations for basic vector processing functionalities,
 // including support for "1st tier" vector capabilities; in the case of ARM,
 // this first tier include functions for processors supporting at least the NEON
 // instruction set.

#include <stddef.h>
#include "vec.h"

#ifdef __linux__
    #include <sys/auxv.h>
    #include <asm/hwcap.h>
#endif

#ifdef __APPLE__
#include <TargetConditionals.h>
#endif

EXPORT int vec_caps() {
#ifdef __APPLE__
    #ifdef TARGET_OS_OSX
        // All M series Apple silicon support Neon instructions; no SVE support as for now (M4)
        return 1;
    #else
        #error "Unsupported Apple platform"
    #endif
#elif __linux__
    int hwcap = getauxval(AT_HWCAP);
    int neon = (hwcap & HWCAP_ASIMD) != 0;
    // https://docs.kernel.org/arch/arm64/sve.html
    int sve = (hwcap & HWCAP_SVE) != 0;
    int hwcap2 = getauxval(AT_HWCAP2);
    int sve2 = (hwcap2 & HWCAP2_SVE2) != 0;
    if (neon && sve) {
        return 2;
    }
    return neon;
#else
    #error "Unsupported aarch64 platform"
#endif
}
