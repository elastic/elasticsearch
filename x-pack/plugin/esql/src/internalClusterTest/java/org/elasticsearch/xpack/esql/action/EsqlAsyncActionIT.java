/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

@ClusterScope(numDataNodes = 1, numClientNodes = 1) // Just for testing
public class EsqlAsyncActionIT extends EsqlActionIT {

    @Override
    protected EsqlQueryResponse run(String esqlCommands, QueryPragmas pragmas, QueryBuilder filter) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(esqlCommands);
        request.pragmas(pragmas);
        if (filter != null) {
            request.filter(filter);
        }

        System.out.println("HEGO running with async query NEW");
        request.async(true);
        request.waitForCompletionTimeout(TimeValue.timeValueNanos(1));
        request.keepOnCompletion(false);  // effectively disable; TODO:randomize

        // TODO: what is keep on completion ? This is 5 days by default

        var response = run(request);
        if (response.asyncExecutionId().isPresent()) {
            assertThat(response.isRunning(), is(true));
            assertThat(response.columns(), is(empty())); // no partial results
            assertThat(response.pages(), is(empty()));
            response.close();
            ensureBlocksReleased(); // TODO: remove, this is already done elsewhere
            System.out.println("HEGO calling async get");
            return getAsyncResponse(response.asyncExecutionId().get());
        } else {
            System.out.println("HEGO result immediate");
            return response;
        }
    }

    EsqlQueryResponse getAsyncResponse(String id) {
        try {
            GetAsyncResultRequest getResultsRequest = new GetAsyncResultRequest(id).setWaitForCompletionTimeout(
                TimeValue.timeValueSeconds(60)
            );
            return client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }
}
