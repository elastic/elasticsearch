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
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

@com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 3)
public class EsqlAsyncActionIT extends EsqlActionIT {

    @Override
    protected EsqlQueryResponse run(String esqlCommands, QueryPragmas pragmas, QueryBuilder filter) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(esqlCommands);
        request.pragmas(pragmas);
        request.async(true);
        // deliberately small timeout, to frequently trigger incomplete response
        request.waitForCompletionTimeout(TimeValue.timeValueNanos(1));
        request.keepOnCompletion(randomBoolean());
        if (filter != null) {
            request.filter(filter);
        }

        var response = run(request);
        if (response.asyncExecutionId().isPresent()) {
            assertThat(response.isRunning(), is(true));
            assertThat(response.columns(), is(empty())); // no partial results
            assertThat(response.pages(), is(empty()));
            response.close();
            return getAsyncResponse(response.asyncExecutionId().get());
        } else {
            return response;
        }
    }

    EsqlQueryResponse getAsyncResponse(String id) {
        try {
            GetAsyncResultRequest getResultsRequest = new GetAsyncResultRequest(id).setWaitForCompletionTimeout(
                TimeValue.timeValueSeconds(60)
            );
            var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).actionGet(30, TimeUnit.SECONDS);
            // resp.decRef(); // the client has incremented our non-0 resp
            return resp;
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/102455")
    // junit.framework.AssertionFailedError: Unexpected exception type, expected VerificationException but got
    // org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper: verification_exception: Found 1 problem
    @Override
    public void testOverlappingIndexPatterns() throws Exception {
        super.testOverlappingIndexPatterns();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/102455")
    @Override
    public void testIndexPatterns() throws Exception {
        super.testOverlappingIndexPatterns();
    }
}
