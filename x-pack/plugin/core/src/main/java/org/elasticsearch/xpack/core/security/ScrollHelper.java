/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public final class ScrollHelper {

    private static final Logger LOGGER = LogManager.getLogger(ScrollHelper.class);

    private ScrollHelper() {}

    /**
     * This method fetches all results for the given search request, parses them using the given hit parser and calls the
     * listener once done.
     */
    public static <T> void fetchAllByEntity(
        Client client,
        SearchRequest request,
        final ActionListener<Collection<T>> listener,
        Function<SearchHit, T> hitParser
    ) {
        final List<T> results = new ArrayList<>();
        if (request.scroll() == null) { // we do scroll by default lets see if we can get rid of this at some point.
            throw new IllegalArgumentException("request must have scroll set");
        }
        final Consumer<SearchResponse> clearScroll = (response) -> {
            if (response != null && response.getScrollId() != null) {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(response.getScrollId());
                client.clearScroll(
                    clearScrollRequest,
                    ActionListener.wrap(
                        (r) -> {},
                        e -> LOGGER.warn(() -> "clear scroll failed for scroll id [" + response.getScrollId() + "]", e)
                    )
                );
            }
        };
        // This function is MADNESS! But it works, don't think about it too hard...
        // simon edit: just watch this if you got this far https://www.youtube.com/watch?v=W-lF106Dgk8
        client.search(
            request,
            new ContextPreservingActionListener<>(
                client.threadPool().getThreadContext().newRestorableContext(true),
                new ActionListener<SearchResponse>() {
                    private volatile SearchResponse lastResponse = null;

                    @Override
                    public void onResponse(SearchResponse resp) {
                        try {
                            lastResponse = resp;
                            if (resp.getHits().getHits().length > 0) {
                                for (SearchHit hit : resp.getHits().getHits()) {
                                    final T oneResult = hitParser.apply(hit);
                                    if (oneResult != null) {
                                        results.add(oneResult);
                                    }
                                }

                                if (results.size() > resp.getHits().getTotalHits().value()) {
                                    clearScroll.accept(lastResponse);
                                    listener.onFailure(
                                        new IllegalStateException(
                                            "scrolling returned more hits ["
                                                + results.size()
                                                + "] than expected ["
                                                + resp.getHits().getTotalHits().value()
                                                + "] so bailing out to prevent unbounded "
                                                + "memory consumption."
                                        )
                                    );
                                } else if (results.size() == resp.getHits().getTotalHits().value()) {
                                    clearScroll.accept(resp);
                                    // Finally, return the list of the entity
                                    listener.onResponse(Collections.unmodifiableList(results));
                                } else {
                                    SearchScrollRequest scrollRequest = new SearchScrollRequest(resp.getScrollId());
                                    scrollRequest.scroll(request.scroll());
                                    client.searchScroll(scrollRequest, this);
                                }
                            } else {
                                clearScroll.accept(resp);
                                // Finally, return the list of the entity
                                listener.onResponse(Collections.unmodifiableList(results));
                            }
                        } catch (Exception e) {
                            onFailure(e); // lets clean up things
                        }
                    }

                    @Override
                    public void onFailure(Exception t) {
                        try {
                            // attempt to clear the scroll request
                            clearScroll.accept(lastResponse);
                        } finally {
                            if (t instanceof IndexNotFoundException) {
                                // since this is expected to happen at times, we just call the listener with an empty list
                                listener.onResponse(Collections.<T>emptyList());
                            } else {
                                listener.onFailure(t);
                            }
                        }
                    }
                }
            )
        );
    }
}
