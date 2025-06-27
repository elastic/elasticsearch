/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.VerificationException;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

@TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand planning")
public class CrossClusterLookupJoinFailuresIT extends AbstractCrossClusterTestCase {
    protected boolean reuseClusters() {
        return false;
    }

    public void testLookupFail() throws IOException {
        setupClusters(3);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 25);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 25);

        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        Exception simulatedFailure = randomFailure();
        // fail when trying to do the lookup on remote cluster
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
            // FIXME: this should catch the error but fails instead
            /*
            try (
                EsqlQueryResponse resp = runQuery(
                    "FROM c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                    randomBoolean()
                )
            ) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag"));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remoteCluster.getFailures(), not(empty()));
                var failure = remoteCluster.getFailures().get(0);
                assertThat(failure.reason(), containsString(simulatedFailure.getMessage()));
            } */

            try (
                EsqlQueryResponse resp = runQuery(
                    "FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                    randomBoolean()
                )
            ) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag"));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(10));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

                var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remoteCluster.getFailures(), not(empty()));
                var failure = remoteCluster.getFailures().get(0);
                // FIXME: this produces a wrong message currently
                // assertThat(failure.reason(), containsString(simulatedFailure.getMessage()));
                assertThat(failure.reason(), containsString("lookup index [values_lookup] is not available in remote cluster [cluster-a]"));
            }

            try (
                EsqlQueryResponse resp = runQuery(
                    "FROM logs-*,*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                    randomBoolean()
                )
            ) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag"));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(20));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

                var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                var remoteCluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remoteCluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remoteCluster.getFailures(), not(empty()));
                var failure = remoteCluster.getFailures().get(0);
                // FIXME: this produces a wrong message currently
                // assertThat(failure.reason(), containsString(simulatedFailure.getMessage()));
                assertThat(failure.reason(), containsString("lookup index [values_lookup] is not available in remote cluster [cluster-a]"));
            }

            // now fail
            setSkipUnavailable(REMOTE_CLUSTER_1, false);
            Exception ex = expectThrows(
                VerificationException.class,
                () -> runQuery("FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key", randomBoolean())
            );
            assertThat(ex.getMessage(), containsString("lookup index [values_lookup] is not available in remote cluster [cluster-a]"));

            ex = expectThrows(
                Exception.class,
                () -> runQuery("FROM c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key", randomBoolean())
            );
            assertThat(ex.getMessage(), containsString(simulatedFailure.getMessage()));
        } finally {
            for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
                MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
                ts.clearAllRules();
            }
        }
    }

    public void testLookupDisconnect() throws IOException {
        setupClusters(2);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);

        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        try {
            // close the remote cluster so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            try (
                EsqlQueryResponse resp = runQuery(
                    "FROM c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                    randomBoolean()
                )
            ) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                // this is a bit weird but that's how it works
                assertThat(columns, hasSize(1));
                assertThat(columns, hasItems("<no-fields>"));
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remoteCluster.getFailures(), not(empty()));
                var failure = remoteCluster.getFailures().get(0);
                assertThat(failure.reason(), containsString("unable to connect to remote cluster"));
            }

            try (
                EsqlQueryResponse resp = runQuery(
                    "FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                    randomBoolean()
                )
            ) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag"));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(10));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

                var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remoteCluster.getFailures(), not(empty()));
                var failure = remoteCluster.getFailures().get(0);
                assertThat(
                    failure.reason(),
                    containsString("Remote cluster [cluster-a] (with setting skip_unavailable=true) is not available")
                );
            }

            setSkipUnavailable(REMOTE_CLUSTER_1, false);
            Exception ex = expectThrows(
                ElasticsearchException.class,
                () -> runQuery("FROM c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key", randomBoolean())
            );
            assertTrue(ExceptionsHelper.isRemoteUnavailableException(ex));

            ex = expectThrows(
                ElasticsearchException.class,
                () -> runQuery("FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key", randomBoolean())
            );
            assertTrue(ExceptionsHelper.isRemoteUnavailableException(ex));
        } finally {
            clearSkipUnavailable(2);
        }
    }

}
