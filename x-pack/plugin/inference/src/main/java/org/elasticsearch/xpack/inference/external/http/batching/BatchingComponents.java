/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.Retrier;

import java.util.Objects;

// TODO I think we can remove the threadpool?
public record BatchingComponents(Retrier retrier, ThreadPool threadPool) {
    public BatchingComponents {
        Objects.requireNonNull(retrier);
        Objects.requireNonNull(threadPool);
    }
}
