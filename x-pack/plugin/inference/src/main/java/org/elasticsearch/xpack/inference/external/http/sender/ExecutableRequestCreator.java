/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;

import java.util.List;
import java.util.function.Supplier;

/**
 * A contract for constructing a {@link Runnable} to handle sending an inference request to a 3rd party service.
 */
public interface ExecutableRequestCreator {
    Runnable create(
        List<String> input,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        HttpClientContext context,
        ActionListener<InferenceServiceResults> listener
    );
}
