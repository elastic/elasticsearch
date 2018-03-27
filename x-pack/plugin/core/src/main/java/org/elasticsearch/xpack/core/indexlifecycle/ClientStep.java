/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.StepResult;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ClientStep<RequestBuilder extends ActionRequestBuilder, Response extends ActionResponse> extends Step {

    private final RequestBuilder requestBuilder;
    private final Function<ClusterState, Boolean> checkComplete;
    private final Function<Response, Boolean> checkSuccess;
    private Exception returnedException;
    private boolean returnedSuccess;

    public ClientStep(String name, String action, String phase, String index, Step nextStep, RequestBuilder requestBuilder,
                      Function<ClusterState, Boolean> checkComplete, Function<Response, Boolean> checkSuccess) {
        super(name, action, phase, nextStep);
        this.requestBuilder = requestBuilder;
        this.checkComplete = checkComplete;
        this.checkSuccess = checkSuccess;
        this.returnedException = null;
        this.returnedSuccess = false;
    }
}
