/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

#ifdef _MSC_VER
#define EXPORT extern "C" __declspec(dllexport)
#elif defined(__GNUC__) && !defined(__clang__)
#define EXPORT extern "C" __attribute__((externally_visible,visibility("default")))
#elif __clang__
#define EXPORT extern "C" __attribute__((visibility("default")))
#endif


#ifdef __cplusplus
    #if (__cplusplus >= 202302L) && (!__clang__)
        // Compiler is C++ and supports the C++23 floating-point definitions
        #include <stdfloat>
        #if __STDCPP_FLOAT32_T__ != 1
            #error "32-bit float type required"
        #endif
        #define f32_t std::float32_t

        #if __STDCPP_BFLOAT16_T__ != 1
            #error "bfloat16 type required"
        #endif
        #define bf16_t std::bfloat16_t
    #elif (__cplusplus >= 201103L)
        // Compiler is C++ and support C++11 static assert
        // Define our own 32-bit float type as float, but check the dimension is correct, or fail
        static_assert(sizeof(float) == 4, "Unsupported compiler. Please define f32_t to designate a 32-bit float.");
        #define f32_t float

        #include <cstdint>
        // use uint16 as something that is guaranteed to be 16 bits wide
        // we'll need to cast to a float type to do scalar ops on it
        static_assert(sizeof(uint16_t) == 2, "Unsupported compiler. Please define bf16_t to designate a 16-bit bfloat.");
        #define bf16_t uint16_t
    #else
        #error "Unsupported compiler. Please define f32_t and bf16_t."
    #endif
#else
    #error "This library is meant to be compiled with a C++ compiler"
#endif

