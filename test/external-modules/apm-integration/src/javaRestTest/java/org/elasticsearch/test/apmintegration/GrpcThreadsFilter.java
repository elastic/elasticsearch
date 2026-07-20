/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Excludes gRPC's shared daemon threads from the thread-leak checker.
 * {@code grpc-default-executor-N} and {@code grpc-shared-destroyer-N} are managed by gRPC's
 * {@code SharedResourceHolder}: the executor is reclaimed on a delay after the {@link RecordingApmServer}
 * gRPC server shuts down, and the destroyer is a JVM-wide singleton daemon. Neither is tied to the
 * server lifecycle, so they linger past teardown without being a true resource leak.
 */
public class GrpcThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        String name = t.getName();
        return name.startsWith("grpc-default-executor-") || name.startsWith("grpc-shared-destroyer-");
    }
}
