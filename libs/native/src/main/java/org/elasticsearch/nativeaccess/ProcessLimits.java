/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

/**
 * Limits for the current process.
 *
 * @param maxThreads The maximum number of threads that may be created.
 * @param maxVirtualMemorySize The maximum size of virtual memory.
 * @param maxFileSize The maximum size of a single file.
 */
public record ProcessLimits(long maxThreads, long maxVirtualMemorySize, long maxFileSize) {
    public static final long UNKNOWN = -1;
    public static final long UNLIMITED = Long.MAX_VALUE;
}
