/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    public static void startAsyncQuery(
        Client client,
        String q,
        AtomicReference<String> asyncExecutionId,
        Tuple<Boolean, Boolean> includeCCSMetadata
    ) {
        try (EsqlQueryResponse resp = runAsyncQuery(client, q, includeCCSMetadata.v1(), null, TimeValue.timeValueMillis(100))) {
            assertTrue(resp.isRunning());
            assertNotNull("async execution id is null", resp.asyncExecutionId());
            asyncExecutionId.set(resp.asyncExecutionId().get());
            // executionInfo may or may not be set on the initial response when there is a relatively low wait_for_completion_timeout
            // so we do not check for it here
        }
    }

    public static EsqlQueryResponse runAsyncQuery(
        Client client,
        String query,
        Boolean ccsMetadata,
        QueryBuilder filter,
        TimeValue waitCompletionTime
    ) {
        EsqlQueryRequest request = EsqlQueryRequest.asyncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        if (ccsMetadata != null) {
            request.includeCCSMetadata(ccsMetadata);
        }
        request.waitForCompletionTimeout(waitCompletionTime);
        request.keepOnCompletion(true);
        if (filter != null) {
            request.filter(filter);
        }
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
                assertThat(clusterInfo.getStatus(), not(equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING)));
            }
        });
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
