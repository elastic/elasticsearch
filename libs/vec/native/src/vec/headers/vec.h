/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

#ifdef _MSC_VER
#define EXPORT extern "C" __declspec(dllexport)
#elif defined(__GNUC__) && !defined(__clang__)
#define EXPORT __attribute__((externally_visible,visibility("default")))
#elif __clang__
#define EXPORT __attribute__((visibility("default")))
#endif

EXPORT int vec_caps();

EXPORT int32_t dot7u(int8_t* a, int8_t* b, size_t dims);

EXPORT int32_t sqr7u(int8_t *a, int8_t *b, size_t length);
