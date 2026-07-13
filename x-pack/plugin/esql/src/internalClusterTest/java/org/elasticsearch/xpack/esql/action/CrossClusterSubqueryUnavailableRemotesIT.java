/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.CrossClusterSubqueryIT.assertClusterEsqlExecutionInfo;
import static org.elasticsearch.xpack.esql.action.CrossClusterSubqueryIT.assertClusterEsqlExecutionInfoFailureReason;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

//@TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand error handling")
public class CrossClusterSubqueryUnavailableRemotesIT extends AbstractCrossClusterTestCase {

    @Before
    public void checkSubqueryInFromCommandSupport() throws IOException {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        setupClusters(3);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    /*
     * Test that when skip_unavailable is true, and a remote cluster fails to respond,
     * the query does not error out and data is returned from the other clusters.
     */
    public void testSubqueryFailToReceiveClusterResponseWithSkipUnavailableTrue() {
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);
        Exception simulatedFailure = mockTransportServiceToReceiveSimulatedFailure();

        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM logs-*,(FROM *:logs-*)
                | KEEP v, tag
                """, randomBoolean())) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                assertThat(columns, hasItems("v", "tag"));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(20));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertClusterEsqlExecutionInfo(executionInfo, LOCAL_CLUSTER, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_1, EsqlExecutionInfo.Cluster.Status.SKIPPED);
                assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_2, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                assertClusterEsqlExecutionInfoFailureReason(executionInfo, REMOTE_CLUSTER_1, simulatedFailure.getMessage());
            }

            // This is how it behaves today. If all the remote clusters referenced by a subquery fail to respond,
            // the whole query fails even if there are other subqueries and their local/remote clusters are available.
            // The mock TransportationService does not cause a trouble to IndexResolution.
            Exception ex = expectThrows(Exception.class, () -> runQuery("""
                FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)
                | KEEP v, tag
                """, randomBoolean()));
            String message = ex.getCause() == null ? ex.getMessage() : ex.getCause().getMessage();
            assertThat(message, containsString(simulatedFailure.getMessage()));
        } finally {
            for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
                MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
                ts.clearAllRules();
            }
        }
    }

    /*
     * Test that when skip_unavailable is false, and a remote cluster fails to respond, the query fails.
     */
    public void testSubqueryFailToReceiveClusterResponseWithSkipUnavailableFalse() {
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        Exception simulatedFailure = mockTransportServiceToReceiveSimulatedFailure();

        try {
            // with local indices
            Exception ex = expectThrows(Exception.class, () -> runQuery("""
                FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)
                | KEEP v, tag
                """, randomBoolean()));
            String message = ex.getCause() == null ? ex.getMessage() : ex.getCause().getMessage();
            assertThat(message, containsString(simulatedFailure.getMessage()));

            // without local indices
            ex = expectThrows(Exception.class, () -> runQuery("""
                FROM (FROM c*:logs-*), (FROM r*:logs-*)
                | KEEP v, tag
                """, randomBoolean()));
            message = ex.getCause() == null ? ex.getMessage() : ex.getCause().getMessage();
            assertThat(message, containsString(simulatedFailure.getMessage()));
        } finally {
            for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
                MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
                ts.clearAllRules();
            }
        }
    }

    /*
     * Test that when skip_unavailable is true, a disconnected remote cluster is skipped.
     */
    public void testSubqueryWithDisconnectedRemoteClusterWithSkipUnavailableTrue() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);
        try {
            // close the remote cluster so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            try (EsqlQueryResponse resp = runQuery("""
                FROM logs-*,(FROM *:logs-*)
                | KEEP v, tag
                """, randomBoolean())) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                assertThat(columns, hasItems("v", "tag"));
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(20));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertClusterEsqlExecutionInfo(executionInfo, LOCAL_CLUSTER, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_1, EsqlExecutionInfo.Cluster.Status.SKIPPED);
                assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_2, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                assertClusterEsqlExecutionInfoFailureReason(
                    executionInfo,
                    REMOTE_CLUSTER_1,
                    "Remote cluster [cluster-a] (with setting skip_unavailable=true) is not available"
                );
            }

            // This is how it behaves today, if the remote cluster that is referenced by a subquery is not available,
            // and there is no other cluster available in the same subquery, then no data is returned, no exception is thrown.
            try (EsqlQueryResponse resp = runQuery("""
                FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)
                | KEEP v, tag
                """, randomBoolean())) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                assertThat(columns, hasItems("<no-fields>"));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertEquals(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, localCluster.getStatus());
                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertEquals(EsqlExecutionInfo.Cluster.Status.SKIPPED, remoteCluster.getStatus());
                assertThat(remoteCluster.getFailures(), not(empty()));
                var failure = remoteCluster.getFailures().get(0);
                assertThat(failure.reason(), containsString("unable to connect to remote cluster"));
            }
        } finally {
            clearSkipUnavailable(3);
        }
    }

    /*
     * Test that when skip_unavailable is false, a disconnected remote cluster causes the query to fail.
     */
    public void testSubqueryWithDisconnectedRemoteClusterWithSkipUnavailableFalse() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        try {
            // close the remote cluster so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            Exception ex = expectThrows(ElasticsearchException.class, () -> runQuery("""
                FROM logs-*,(FROM *:logs-*)
                | KEEP v, tag
                """, randomBoolean()));
            assertTrue(ExceptionsHelper.isRemoteUnavailableException(ex));

            ex = expectThrows(ElasticsearchException.class, () -> runQuery("""
                FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)
                | KEEP v, tag
                """, randomBoolean()));
            assertTrue(ExceptionsHelper.isRemoteUnavailableException(ex));

            ex = expectThrows(ElasticsearchException.class, () -> runQuery("""
                FROM (FROM c*:logs-*), (FROM r*:logs-*)
                | KEEP v, tag
                """, randomBoolean()));
            assertTrue(ExceptionsHelper.isRemoteUnavailableException(ex));
        } finally {
            clearSkipUnavailable(3);
        }
    }

    /*
     * Mocks a bad REMOTE_CLUSTER_1.
     */
    private Exception mockTransportServiceToReceiveSimulatedFailure() {
        Exception simulatedFailure = randomFailure();
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
        return simulatedFailure;
    }
}
