/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.indexlifecycle.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.indexlifecycle.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.indexlifecycle.StartILMRequest;
import org.elasticsearch.xpack.core.indexlifecycle.StopILMRequest;
import org.elasticsearch.xpack.core.indexlifecycle.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetStatusAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.RemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.RetryAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.StartILMAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.StopILMAction;

/**
 * A wrapper to elasticsearch clients that exposes all ILM related APIs
 */
public class ILMClient {

    private ElasticsearchClient client;

    public ILMClient(ElasticsearchClient client) {
        this.client = client;
    }

    /**
     * Create or modify a lifecycle policy definition
     */
    public void putLifecyclePolicy(PutLifecycleAction.Request request, ActionListener<PutLifecycleAction.Response> listener) {
        client.execute(PutLifecycleAction.INSTANCE, request, listener);
    }

    /**
     * Create or modify a lifecycle policy definition
     */
    public ActionFuture<PutLifecycleAction.Response> putLifecyclePolicy(PutLifecycleAction.Request request) {
        return client.execute(PutLifecycleAction.INSTANCE, request);
    }

    /**
     * Get a lifecycle policy definition
     */
    public void getLifecyclePolicy(GetLifecycleAction.Request request, ActionListener<GetLifecycleAction.Response> listener) {
        client.execute(GetLifecycleAction.INSTANCE, request, listener);
    }

    /**
     * Get a lifecycle policy definition
     */
    public ActionFuture<GetLifecycleAction.Response> getLifecyclePolicy(GetLifecycleAction.Request request) {
        return client.execute(GetLifecycleAction.INSTANCE, request);
    }

    /**
     * Delete a lifecycle policy definition
     */
    public void deleteLifecyclePolicy(DeleteLifecycleAction.Request request, ActionListener<DeleteLifecycleAction.Response> listener) {
        client.execute(DeleteLifecycleAction.INSTANCE, request, listener);
    }

    /**
     * Delete a lifecycle policy definition
     */
    public ActionFuture<DeleteLifecycleAction.Response> deleteLifecyclePolicy(DeleteLifecycleAction.Request request) {
        return client.execute(DeleteLifecycleAction.INSTANCE, request);
    }

    /**
     * Explain the current lifecycle state for an index
     */
    public void explainLifecycle(ExplainLifecycleRequest request, ActionListener<ExplainLifecycleResponse> listener) {
        client.execute(ExplainLifecycleAction.INSTANCE, request, listener);
    }

    /**
     * Explain the current lifecycle state for an index
     */
    public ActionFuture<ExplainLifecycleResponse> explainLifecycle(ExplainLifecycleRequest request) {
        return client.execute(ExplainLifecycleAction.INSTANCE, request);
    }

    /**
     * Returns the current status of the ILM plugin
     */
    public void getStatus(GetStatusAction.Request request, ActionListener<GetStatusAction.Response> listener) {
        client.execute(GetStatusAction.INSTANCE, request, listener);
    }

    /**
     * Returns the current status of the ILM plugin
     */
    public ActionFuture<GetStatusAction.Response> getStatus(GetStatusAction.Request request) {
        return client.execute(GetStatusAction.INSTANCE, request);
    }

    /**
     * Removes index lifecycle management from an index
     */
    public void removeIndexLifecyclePolicy(RemoveIndexLifecyclePolicyAction.Request request,
            ActionListener<RemoveIndexLifecyclePolicyAction.Response> listener) {
        client.execute(RemoveIndexLifecyclePolicyAction.INSTANCE, request, listener);
    }

    /**
     * Removes index lifecycle management from an index
     */
    public ActionFuture<RemoveIndexLifecyclePolicyAction.Response> removeIndexLifecyclePolicy(
            RemoveIndexLifecyclePolicyAction.Request request) {
        return client.execute(RemoveIndexLifecyclePolicyAction.INSTANCE, request);
    }

    /**
     * Retries the policy for an index which is currently in ERROR
     */
    public void retryPolicy(RetryAction.Request request, ActionListener<RetryAction.Response> listener) {
        client.execute(RetryAction.INSTANCE, request, listener);
    }

    /**
     * Removes index lifecycle management from an index
     */
    public ActionFuture<RetryAction.Response> retryPolicy(RetryAction.Request request) {
        return client.execute(RetryAction.INSTANCE, request);
    }

    /**
     * Starts the ILM plugin
     */
    public void startILM(StartILMRequest request, ActionListener<AcknowledgedResponse> listener) {
        client.execute(StartILMAction.INSTANCE, request, listener);
    }

    /**
     * Starts the ILM plugin
     */
    public ActionFuture<AcknowledgedResponse> startILM(StartILMRequest request) {
        return client.execute(StartILMAction.INSTANCE, request);
    }

    /**
     * Stops the ILM plugin
     */
    public void stopILM(StopILMRequest request, ActionListener<AcknowledgedResponse> listener) {
        client.execute(StopILMAction.INSTANCE, request, listener);
    }

    /**
     * Stops the ILM plugin
     */
    public ActionFuture<AcknowledgedResponse> stopILM(StopILMRequest request) {
        return client.execute(StopILMAction.INSTANCE, request);
    }
}
