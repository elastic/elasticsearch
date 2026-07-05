/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.tasks.Task;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link MappedActionFilter}s that fail the continuation step of local bulk-by-scroll search:
 * <ul>
 *     <li><strong>PIT (reindex):</strong> second matching {@link TransportSearchAction} after
 *     {@link org.elasticsearch.index.reindex.ReindexRequest#convertSearchRequestToUsePit} (empty indices, PIT in source).</li>
 *     <li><strong>Scroll (update/delete-by-query):</strong> first {@link TransportSearchScrollAction} — scroll continuations do not
 *     use {@link TransportSearchAction}.</li>
 * </ul>
 */
public final class SearchContextFailureInjectionPlugin extends Plugin implements ActionPlugin {

    /** Second PIT search request is the first continuation page when {@code source.size} splits work across pages. */
    private static final int PIT_SEARCH_TO_FAIL = 2;

    /** First scroll continuation is the first {@link SearchScrollRequest}. */
    private static final int SCROLL_REQUEST_TO_FAIL = 1;

    static final AtomicReference<InjectionConfig> CONFIG = new AtomicReference<>();
    static final AtomicInteger PIT_SEARCH_COUNTER = new AtomicInteger(0);
    static final AtomicInteger SCROLL_SEARCH_COUNTER = new AtomicInteger(0);

    public record InjectionConfig(TimeValue sleepBeforeFail, SearchContextMissingException exception) {}

    @Override
    public Collection<MappedActionFilter> getMappedActionFilters() {
        return List.of(new MappedActionFilter() {
            @Override
            public String actionName() {
                return TransportSearchAction.NAME;
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                InjectionConfig cfg = CONFIG.get();
                if (cfg == null || request instanceof SearchRequest == false) {
                    chain.proceed(task, action, request, listener);
                    return;
                }
                SearchRequest searchRequest = (SearchRequest) request;
                if (matchesBulkByScrollPitContinuationSearch(searchRequest) == false) {
                    chain.proceed(task, action, request, listener);
                    return;
                }
                int n = PIT_SEARCH_COUNTER.incrementAndGet();
                if (n != PIT_SEARCH_TO_FAIL) {
                    chain.proceed(task, action, request, listener);
                    return;
                }
                sleepThenFail(cfg, listener);
            }
        }, new MappedActionFilter() {
            @Override
            public String actionName() {
                return TransportSearchScrollAction.TYPE.name();
            }

            @Override
            public <Req extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Req request,
                ActionListener<Response> listener,
                ActionFilterChain<Req, Response> chain
            ) {
                InjectionConfig cfg = CONFIG.get();
                if (cfg == null || request instanceof SearchScrollRequest == false) {
                    chain.proceed(task, action, request, listener);
                    return;
                }
                int n = SCROLL_SEARCH_COUNTER.incrementAndGet();
                if (n != SCROLL_REQUEST_TO_FAIL) {
                    chain.proceed(task, action, request, listener);
                    return;
                }
                sleepThenFail(cfg, listener);
            }
        });
    }

    private static void sleepThenFail(InjectionConfig cfg, ActionListener<?> listener) {
        assert cfg != null;
        if (cfg.sleepBeforeFail != null && cfg.sleepBeforeFail.millis() > 0) {
            try {
                Thread.sleep(cfg.sleepBeforeFail.millis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.onFailure(new ElasticsearchException(e));
                return;
            }
        }
        listener.onFailure(cfg.exception());
    }

    /**
     * After PIT conversion, continuation searches carry an empty {@link SearchRequest#indices()} array and a non-null
     * point-in-time in the source.
     */
    static boolean matchesBulkByScrollPitContinuationSearch(SearchRequest searchRequest) {
        if (searchRequest.source() == null || searchRequest.source().pointInTimeBuilder() == null) {
            return false;
        }
        return searchRequest.indices().length == 0;
    }
}
