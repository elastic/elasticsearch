/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;

/**
 * A client that delegates to a verifying function for action/request/listener
 */
public class VerifyingClient extends NoOpClient {

    private TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier = (a, r, l) -> {
        Assert.fail("verifier not set");
        return null;
    };

    VerifyingClient(ThreadPool threadPool) {
        super(threadPool);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        try {
            listener.onResponse((Response) verifier.apply(action, request, listener));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public VerifyingClient setVerifier(TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier) {
        this.verifier = verifier;
        return this;
    }
}
