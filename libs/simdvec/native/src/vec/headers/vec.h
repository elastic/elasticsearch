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
    #elif (__cplusplus >= 201103L)
        // Compiler is C++ and support C++11 static assert
        // Define our own 32-bit float type as float, but check the dimension is correct, or fail
        static_assert(sizeof(float) == 4, "Unsupported compiler. Please define f32_t to designate a 32-bit float.");
        #define f32_t float
    #else
        #error "Unsupported compiler. Please define f32_t to designate a 32-bit float."
    #endif
#else
    #error "This library is meant to be compiled with a C++ compiler"
#endif

EXPORT int vec_caps();

EXPORT int32_t vec_dot7u(int8_t* a, int8_t* b, const int32_t dims);

EXPORT void vec_dot7u_bulk(int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results);

EXPORT int32_t vec_sqr7u(int8_t *a, int8_t *b, const int32_t length);

EXPORT f32_t vec_cosf32(const f32_t *a, const f32_t *b, const int32_t elementCount);

EXPORT f32_t vec_dotf32(const f32_t *a, const f32_t *b, const int32_t elementCount);

EXPORT f32_t vec_sqrf32(const f32_t *a, const f32_t *b, const int32_t elementCount);

