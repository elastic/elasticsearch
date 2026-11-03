/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/** Every {@link IntoIT} scenario through the async query API: submitted, then fetched by id once it is done. */
public class IntoAsyncIT extends IntoIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), EsqlAsyncActionIT.LocalStateEsqlAsync.class);
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest original) {
        EsqlQueryRequest request = EsqlQueryRequest.asyncEsqlQueryRequest(original.query());
        request.pragmas(original.pragmas());
        request.acceptedPragmaRisks(true);
        // Deliberately small, so the write usually outlives the submit and the result has to be fetched.
        request.waitForCompletionTimeout(TimeValue.timeValueNanos(1));
        request.keepOnCompletion(false);

        EsqlQueryResponse response = super.run(request);
        if (response.asyncExecutionId().isEmpty() || response.isRunning() == false) {
            return response;
        }
        String id = response.asyncExecutionId().get();
        response.close();
        try {
            var get = new GetAsyncResultRequest(id).setWaitForCompletionTimeout(TimeValue.timeValueSeconds(60));
            return client().execute(EsqlAsyncGetResultAction.INSTANCE, get).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }
}
