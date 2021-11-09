/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.VersionMismatchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ql.util.Holder;

import static org.elasticsearch.action.ActionListener.wrap;

/**
 * A base class for operations that will be retried in case first attempt failed with a {@code VersionMismatchException}.
 * The second attempt will be performed on a node of an older version.
 */
public final class TransportRetryAction<Request extends ActionRequest, Response extends ActionResponse> {

    private final Logger log = LogManager.getLogger(getClass());
    private final RetryOperation<Request, Response> retryOperation;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final String actionName;
    private final Writeable.Reader<Response> responseReader;

    public interface RetryOperation<Request extends ActionRequest, Response extends ActionResponse> {
        void executeRequest(Task task, Request request, ActionListener<Response> listener);
    }

    public TransportRetryAction(
        RetryOperation<Request, Response> retryOperation,
        TransportService transportService,
        ClusterService clusterService,
        String actionName,
        Writeable.Reader<Response> responseReader
    ) {
        this.retryOperation = retryOperation;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.actionName = actionName;
        this.responseReader = responseReader;
    }

    public void executeWithRetry(Task task, Request request, ActionListener<Response> listener) {
        Holder<Boolean> retrySecondTime = new Holder<Boolean>(false);
        ActionListener<Response> listenerWithRetryAction = wrap(listener::onResponse, e -> {
            // the search request likely ran on nodes with different versions of ES
            // we will retry on a node with an older version that should generate a backwards compatible _search request
            if (e instanceof SearchPhaseExecutionException
                && ((SearchPhaseExecutionException) e).getCause() instanceof VersionMismatchException) {
                if (log.isDebugEnabled()) {
                    log.debug("Caught exception type [{}] with cause [{}].", e.getClass().getName(), e.getCause());
                }
                DiscoveryNode localNode = clusterService.state().nodes().getLocalNode();
                DiscoveryNode candidateNode = null;
                for (DiscoveryNode node : clusterService.state().nodes()) {
                    // find the first node that's older than the current node
                    if (node != localNode && node.getVersion().before(localNode.getVersion())) {
                        candidateNode = node;
                        break;
                    }
                }
                if (candidateNode != null) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                            "Candidate node to resend the request to: address [{}], id [{}], name [{}], version [{}]",
                            candidateNode.getAddress(),
                            candidateNode.getId(),
                            candidateNode.getName(),
                            candidateNode.getVersion()
                        );
                    }
                    // re-send the request to the older node
                    executeRetryRequest(candidateNode, request, listener);
                } else {
                    retrySecondTime.set(true);
                }
            } else {
                listener.onFailure(e);
            }
        });
        retryOperation.executeRequest(task, request, listenerWithRetryAction);
        if (retrySecondTime.get()) {
            if (log.isDebugEnabled()) {
                log.debug("No candidate node found, likely all were upgraded in the meantime. Re-trying the original request.");
            }
            retryOperation.executeRequest(task, request, listener);
        }
    }

    private void executeRetryRequest(DiscoveryNode candidateNode, Request request, ActionListener<Response> listener) {
        transportService.sendRequest(candidateNode, actionName, request, new ActionListenerResponseHandler<>(listener, responseReader));
    }
}
