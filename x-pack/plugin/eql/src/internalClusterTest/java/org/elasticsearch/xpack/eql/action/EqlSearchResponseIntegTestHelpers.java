/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.eql.plugin.EqlAsyncGetResultAction;

import java.util.concurrent.ExecutionException;

/**
 * Helpers for internal-cluster tests that assert on {@link EqlSearchResponse} payloads.
 * <p>
 * {@link Client#execute} returns a future whose {@code onResponse} uses {@code RefCountedFuture} for
 * {@link org.elasticsearch.core.RefCounted} results: the future {@code mustIncRef}s the response, and the caller of
 * {@link java.util.concurrent.Future#get()} must {@link EqlSearchResponse#decRef()} when finished (see
 * {@code AbstractClient.RefCountedFuture}). Use {@code try/finally} with {@link #decRef} after {@code .get()}.
 */
final class EqlSearchResponseIntegTestHelpers {

    private EqlSearchResponseIntegTestHelpers() {}

    static void decRef(EqlSearchResponse response) {
        if (response != null) {
            response.decRef();
        }
    }

    /**
     * Starts an async EQL search (non-blocking initial response), polls with {@link EqlAsyncGetResultAction} until
     * {@link EqlSearchResponse#isRunning()} is false, then runs {@code onFinal} while the last response is still valid
     * for refcounting purposes.
     */
    static void pollAsyncEqlToCompletion(Client client, EqlSearchRequest request, CheckedConsumer<EqlSearchResponse, Exception> onFinal)
        throws Exception {
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        client.execute(EqlSearchAction.INSTANCE, request, ActionListener.wrap(r -> pollAsync(client, r, onFinal, done), done::onFailure));
        finishAsyncFuture(done);
    }

    private static void pollAsync(
        Client client,
        EqlSearchResponse response,
        CheckedConsumer<EqlSearchResponse, Exception> onFinal,
        PlainActionFuture<Void> done
    ) {
        if (response.isRunning() == false) {
            try {
                onFinal.accept(response);
                done.onResponse(null);
            } catch (AssertionError e) {
                done.onFailure(new RuntimeException(e));
            } catch (Exception e) {
                done.onFailure(e);
            }
        } else {
            GetAsyncResultRequest getResultsRequest = new GetAsyncResultRequest(response.id()).setKeepAlive(TimeValue.timeValueMinutes(10))
                .setWaitForCompletionTimeout(TimeValue.timeValueMillis(10));
            client.execute(
                EqlAsyncGetResultAction.INSTANCE,
                getResultsRequest,
                ActionListener.wrap(r -> pollAsync(client, r, onFinal, done), done::onFailure)
            );
        }
    }

    private static void finishAsyncFuture(PlainActionFuture<Void> done) throws Exception {
        try {
            done.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            propagateExecutionException(e);
        }
    }

    private static void propagateExecutionException(ExecutionException e) throws Exception {
        Throwable c = e.getCause();
        if (c instanceof RuntimeException re && re.getCause() instanceof AssertionError ae) {
            throw ae;
        }
        if (c instanceof AssertionError ae) {
            throw ae;
        }
        if (c instanceof Exception ex) {
            throw ex;
        }
        throw new AssertionError(c);
    }
}
