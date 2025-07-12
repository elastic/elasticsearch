/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class CrossClusterQueryFailsIT extends AbstractCrossClusterTestCase {

    private static Exception randomFailure() {
        return randomFrom(
            new IllegalStateException("driver was closed already"),
            new CircuitBreakingException("low memory", CircuitBreaker.Durability.PERMANENT),
            new IOException("broken disk"),
            new ResourceNotFoundException("index not found"),
            new EsRejectedExecutionException("node is shutting down")
        );
    }

    public void testErrorDuringIndexLookupLocalRemote() throws Exception {
        setupClusters(2);
        Exception simulatedFailure = randomFailure();
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        // This will generate a random failure on resolution of remote index
        for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
            MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
            ts.addRequestHandlingBehavior(
                EsqlResolveFieldsAction.RESOLVE_REMOTE_TYPE.name(),
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        sendResponse(simulatedFailure);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        channel.sendResponse(exception);
                    }
                }, task)
            );
        }
        try {
            try (EsqlQueryResponse resp = runQuery("FROM logs-*,c*:logs-* | LIMIT 1", randomBoolean())) {
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                // FIXME: currently this fails, the remote error gets hidden and is not reported in the response
                // field caps response does contain FieldCapabilitiesFailure but it is ignored by mergedMappings
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remoteCluster.getFailures(), not(empty()));
                var failure = remoteCluster.getFailures().get(0);
                assertThat(failure.reason(), containsString(simulatedFailure.getMessage()));
            }
        } finally {
            for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
                MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
                ts.clearAllRules();
            }

        }
    }

    public void testErrorDuringIndexLookupRemoteOnly() throws Exception {
        setupClusters(2);
        Exception simulatedFailure = randomFailure();
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        // This will generate a random failure on resolution of remote index
        for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
            MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
            ts.addRequestHandlingBehavior(
                EsqlResolveFieldsAction.RESOLVE_REMOTE_TYPE.name(),
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        sendResponse(simulatedFailure);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        channel.sendResponse(exception);
                    }
                }, task)
            );
        }
        try {
            // FIXME: this throws exception even though skip_unavailable is set to true on the remote cluster
            // It should have caught it and returned an empty response with a failure in metadata
            try (EsqlQueryResponse resp = runQuery("FROM c*:logs-* | LIMIT 1", randomBoolean())) {
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.getClusters().keySet(), hasSize(1));
                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remoteCluster.getFailures(), not(empty()));
                var failure = remoteCluster.getFailures().get(0);
                assertThat(failure.reason(), containsString(simulatedFailure.getMessage()));
            }
        } finally {
            for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
                MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
                ts.clearAllRules();
            }

        }
    }
}
