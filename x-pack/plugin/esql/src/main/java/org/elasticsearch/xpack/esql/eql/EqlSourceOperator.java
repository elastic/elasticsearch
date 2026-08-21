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
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;

import java.util.List;

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

    public record Factory(
        Client client,
        EqlSearchRequest request,
        EqlRelation.Mode mode,
        List<Attribute> schema,
        Source source,
        boolean warnOnTruncation
    ) implements SourceOperatorFactory {
        @Override
        public String describe() {
            return "EqlSourceOperator[mode=" + mode + ", indices=" + String.join(",", request.indices()) + "]";
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new EqlSourceOperator(driverContext, client, request, mode, schema, source, warnOnTruncation);
        }
    }

    private final DriverContext driverContext;
    private final Client client;
    private final EqlSearchRequest request;
    private final EqlRelation.Mode mode;
    private final List<Attribute> schema;
    private final Source source;
    // When the request size came from the truncation cap (no LIMIT, no WITH size), warn if the response fills it.
    private final boolean warnOnTruncation;

    private boolean requested;
    private boolean emitted;
    private SubscribableListener<Void> blocked;
    private volatile Page page;
    private volatile Exception failure;
    // Number of top-level results (events, or sequences/samples) the response carried; set in the response callback.
    private volatile int topLevelCount;
    // Whether the EQL response was partial (a shard failed or timed out). Only possible when the enclosing ES|QL query
    // allows partial results; surfaced as a warning so the incomplete result is not reported as complete.
    private volatile boolean partial;
    // Guards the close()-vs-response-callback race: Driver.drainAndCloseOperators runs before waitForAsyncActions,
    // so close() can fire while the EQL search is still in flight. Mutations of page/closed are synchronized(this).
    private boolean closed;

    private EqlSourceOperator(
        DriverContext driverContext,
        Client client,
        EqlSearchRequest request,
        EqlRelation.Mode mode,
        List<Attribute> schema,
        Source source,
        boolean warnOnTruncation
    ) {
        this.driverContext = driverContext;
        this.client = client;
        this.request = request;
        this.mode = mode;
        this.schema = schema;
        this.source = source;
        this.warnOnTruncation = warnOnTruncation;
    }

    private void ensureRequested() {
        if (requested) {
            return;
        }
        requested = true;
        blocked = new SubscribableListener<>();
        driverContext.addAsyncAction();
        // client.execute delivers every outcome — including a synchronous registration failure such as a
        // TaskCancelledException when the parent task is already banned — through the listener, never by throwing
        // (NodeClient.doExecute forwards those to onFailure), so the async action is always released below.
        client.execute(EqlSearchAction.INSTANCE, request, ActionListener.wrap(response -> {
            try {
                // Convert synchronously inside the callback and copy the bytes we need out of the (ref-counted)
                // response into blocks. We do not own a reference here — the EQL transport action delivers the
                // response via respondAndRelease and releases it once this listener returns — so we must not
                // decRef it ourselves (doing so over-releases).
                topLevelCount = topLevelResultCount(response);
                partial = response.isPartial();
                Page built = EqlPageConverter.toPage(response, mode, schema, driverContext.blockFactory());
                synchronized (this) {
                    if (closed) {
                        // The operator was closed (cancellation / sibling-operator failure) before the response
                        // arrived; release the freshly-built page here or its breaker-accounted blocks leak.
                        built.releaseBlocks();
                    } else {
                        page = built;
                    }
                }
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
            maybeWarnPartial();
            maybeWarnTruncated();
            return result;
        }
        return null;
    }

    /**
     * Emits a truncation warning on the driver thread (never the transport-response thread, whose thread-context
     * headers are not collected) when the size came from the cap and the response filled it — results may be
     * incomplete. Single-shot: this operator emits at most one page.
     */
    private void maybeWarnTruncated() {
        if (warnOnTruncation && topLevelCount >= request.size()) {
            // A row LIMIT can only shrink the size (it is itself capped at the truncation max), so the remedy is the
            // size option or raising the cluster cap — not LIMIT.
            driverContext.createOnlyWarnings(source)
                .registerWarning(
                    "EQL query returned the maximum number of results ["
                        + request.size()
                        + "]; results may be incomplete. Raise the size option or the ["
                        + AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getKey()
                        + "] setting"
                );
        }
    }

    /**
     * Emits a warning on the driver thread when the EQL response was partial (a shard failed or timed out), so an
     * incomplete result is surfaced to the user. This only happens when the enclosing ES|QL query allows partial
     * results; when it does not, the delegate fails the query outright and no page is emitted.
     *
     * <p>Known boundary: this surfaces the partial state as a warning only. It does not yet flip the ES|QL response's
     * {@code is_partial} flag, because a source operator has no handle on the query-level {@code EsqlExecutionInfo};
     * wiring that through is a follow-up (a client keying off {@code is_partial} alone would not see the shard failure).
     */
    private void maybeWarnPartial() {
        if (partial) {
            driverContext.createOnlyWarnings(source)
                .registerWarning("EQL query returned partial results (one or more shards failed or timed out); some events may be missing");
        }
    }

    private int topLevelResultCount(EqlSearchResponse response) {
        var hits = response.hits();
        if (mode == EqlRelation.Mode.EVENT) {
            return hits.events() == null ? 0 : hits.events().size();
        }
        return hits.sequences() == null ? 0 : hits.sequences().size();
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
        synchronized (this) {
            closed = true;
            if (page != null) {
                page.releaseBlocks();
                page = null;
            }
        }
    }
}
