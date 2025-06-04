/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public final class EsqlAsyncTestUtils {
    public static String startAsyncQuery(Client client, String q, Boolean includeCCSMetadata) {
        return startAsyncQueryWithPragmas(client, q, includeCCSMetadata, null);
    }

    public static String startAsyncQueryWithPragmas(Client client, String q, Boolean includeCCSMetadata, Map<String, Object> pragmas) {
        try (EsqlQueryResponse resp = runAsyncQuery(client, q, includeCCSMetadata, TimeValue.timeValueMillis(100), pragmas)) {
            assertTrue(resp.isRunning());
            assertNotNull("async execution id is null", resp.asyncExecutionId());
            // executionInfo may or may not be set on the initial response when there is a relatively low wait_for_completion_timeout
            // so we do not check for it here
            return resp.asyncExecutionId().get();
        }
    }

    public static EsqlQueryResponse runAsyncQuery(Client client, String query, Boolean ccsMetadata, TimeValue waitCompletionTime) {
        return runAsyncQuery(client, query, ccsMetadata, waitCompletionTime, null);
    }

    private static QueryPragmas randomPragmasWithOverride(@Nullable Map<String, Object> pragmas) {
        if (pragmas == null || pragmas.isEmpty()) {
            return AbstractEsqlIntegTestCase.randomPragmas();
        }
        Settings.Builder settings = Settings.builder();
        settings.put(AbstractEsqlIntegTestCase.randomPragmas().getSettings());
        settings.loadFromMap(pragmas);
        return new QueryPragmas(settings.build());
    }

    public static EsqlQueryResponse runAsyncQuery(
        Client client,
        String query,
        Boolean ccsMetadata,
        TimeValue waitCompletionTime,
        @Nullable Map<String, Object> pragmas
    ) {
        EsqlQueryRequest request = EsqlQueryRequest.asyncEsqlQueryRequest();
        request.query(query);
        request.pragmas(randomPragmasWithOverride(pragmas));
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        if (ccsMetadata != null) {
            request.includeCCSMetadata(ccsMetadata);
        }
        request.waitForCompletionTimeout(waitCompletionTime);
        request.keepOnCompletion(true);
        return runAsyncQuery(client, request);
    }

    /**
     * Wait for the cluster to finish running the query.
     */
    public static void waitForCluster(Client client, String clusterName, String asyncExecutionId) throws Exception {
        assertBusy(() -> {
            try (EsqlQueryResponse asyncResponse = getAsyncResponse(client, asyncExecutionId)) {
                EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                assertNotNull(executionInfo);
                EsqlExecutionInfo.Cluster clusterInfo = executionInfo.getCluster(clusterName);
                // the status of the local cluster won't move to SUCCESS until the reduction pipeline is done
                if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterName)
                    && Objects.requireNonNullElse(clusterInfo.getTotalShards(), 0) > 0) {
                    return;
                }
                assertThat(clusterInfo.getStatus(), not(equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING)));
            }
        }, 30, TimeUnit.SECONDS);
    }

    public static EsqlQueryResponse runAsyncQuery(Client client, EsqlQueryRequest request) {
        try {
            return client.execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout waiting for query response", e);
        }
    }

    public static AcknowledgedResponse deleteAsyncId(Client client, String id) {
        try {
            DeleteAsyncResultRequest request = new DeleteAsyncResultRequest(id);
            return client.execute(TransportDeleteAsyncResultAction.TYPE, request).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout waiting for DELETE response", e);
        }
    }

    public static EsqlQueryResponse getAsyncResponse(Client client, String id) {
        try {
            var getResultsRequest = new GetAsyncResultRequest(id).setWaitForCompletionTimeout(timeValueMillis(1));
            return client.execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout waiting for GET async result", e);
        }
    }
}
