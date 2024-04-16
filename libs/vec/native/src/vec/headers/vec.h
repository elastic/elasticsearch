/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

#define EXPORT __attribute__((externally_visible,visibility("default")))

EXPORT int dot8s_stride();

EXPORT int sqr8s_stride();

EXPORT int32_t dot8s(int8_t* a, int8_t* b, size_t dims);

EXPORT int32_t sqr8s(int8_t *a, int8_t *b, size_t length);
