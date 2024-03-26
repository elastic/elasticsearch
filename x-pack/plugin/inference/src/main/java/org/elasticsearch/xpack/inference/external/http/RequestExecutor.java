/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableRequestCreator;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface RequestExecutor {
    void start();

    void shutdown();

    boolean isShutdown();

    boolean isTerminated();

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    void execute(
        ExecutableRequestCreator requestCreator,
        List<String> input,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );
}
