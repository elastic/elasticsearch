/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;

/**
 * Coordinator-local source operator that delegates to the EQL engine: on first poll it issues a single
 * {@code EqlSearchAction} asynchronously, and once the (bounded) response arrives it converts it to one
 * {@link Page} under the {@code EQL} command's fixed schema. The request runs in the caller's security
 * context, so the user's privileges on the EQL target indices apply.
 *
 * <p>The operator is single-shot: it emits at most one page. It never blocks a compute thread — while the
 * search is in flight {@link #isBlocked()} returns a listener the driver waits on, and the async action is
 * tracked on the {@link DriverContext} so the driver is not considered finished prematurely.
 */
public class EqlSourceOperator extends SourceOperator {

    public record Factory(Client client, EqlSearchRequest request, EqlRelation.Mode mode) implements SourceOperatorFactory {
        @Override
        public String describe() {
            return "EqlSourceOperator[mode=" + mode + ", indices=" + String.join(",", request.indices()) + "]";
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new EqlSourceOperator(driverContext, client, request, mode);
        }
    }

    private final DriverContext driverContext;
    private final Client client;
    private final EqlSearchRequest request;
    private final EqlRelation.Mode mode;

    private boolean requested;
    private boolean emitted;
    private SubscribableListener<Void> blocked;
    private volatile Page page;
    private volatile Exception failure;

    public EqlSourceOperator(DriverContext driverContext, Client client, EqlSearchRequest request, EqlRelation.Mode mode) {
        this.driverContext = driverContext;
        this.client = client;
        this.request = request;
        this.mode = mode;
    }

    private void ensureRequested() {
        if (requested) {
            return;
        }
        requested = true;
        blocked = new SubscribableListener<>();
        driverContext.addAsyncAction();
        client.execute(EqlSearchAction.INSTANCE, request, ActionListener.wrap(response -> {
            try {
                // Convert synchronously inside the callback and copy the bytes we need out of the (ref-counted)
                // response into blocks. We do not own a reference here — the EQL transport action delivers the
                // response via respondAndRelease and releases it once this listener returns — so we must not
                // decRef it ourselves (doing so over-releases).
                page = EqlPageConverter.toPage(response, mode, driverContext.blockFactory());
            } catch (Exception e) {
                failure = e;
            } finally {
                driverContext.removeAsyncAction();
                blocked.onResponse(null);
            }
        }, e -> {
            failure = e;
            driverContext.removeAsyncAction();
            blocked.onResponse(null);
        }));
    }

    @Override
    public IsBlockedResult isBlocked() {
        ensureRequested();
        return blocked.isDone() ? NOT_BLOCKED : new IsBlockedResult(blocked, "eql");
    }

    @Override
    public Page getOutput() {
        ensureRequested();
        if (blocked.isDone() == false) {
            return null;
        }
        if (failure != null) {
            Exception e = failure;
            failure = null;
            throw ExceptionsHelper.convertToRuntime(e);
        }
        if (page != null) {
            Page result = page;
            page = null;
            emitted = true;
            return result;
        }
        return null;
    }

    @Override
    public boolean isFinished() {
        // Stay "not finished" while a failure is pending so the driver calls getOutput() and the exception propagates.
        return emitted && failure == null;
    }

    @Override
    public void finish() {
        emitted = true;
    }

    @Override
    public void close() {
        if (page != null) {
            page.releaseBlocks();
            page = null;
        }
    }
}
