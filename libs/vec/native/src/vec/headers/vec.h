/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

#define EXPORT __attribute__((externally_visible,visibility("default")))

#ifndef STRIDE_BYTES_LEN
#define STRIDE_BYTES_LEN 32
#endif

EXPORT int stride();

EXPORT int dot8s(const void* a, const void* b, int dims);
