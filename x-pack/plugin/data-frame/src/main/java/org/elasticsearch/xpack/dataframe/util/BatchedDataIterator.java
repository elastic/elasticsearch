/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Provides basic tools around scrolling over documents and gathering the data in some Collection
 * @param <T> The object type that is being collected
 * @param <E> The collection that should be used (i.e. Set, Deque, etc.)
 */
public abstract class BatchedDataIterator<T, E extends Collection<T>> {
    private static final Logger LOGGER = LogManager.getLogger(BatchedDataIterator.class);

    private static final String CONTEXT_ALIVE_DURATION = "5m";
    private static final int BATCH_SIZE = 10_000;

    private final Client client;
    private final String index;
    private volatile long count;
    private volatile long totalHits;
    private volatile String scrollId;
    private volatile boolean isScrollInitialised;

    protected BatchedDataIterator(Client client, String index) {
        this.client = Objects.requireNonNull(client);
        this.index = Objects.requireNonNull(index);
        this.totalHits = 0;
        this.count = 0;
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    public boolean hasNext() {
        return !isScrollInitialised || count != totalHits;
    }

    /**
     * The first time next() is called, the search will be performed and the first
     * batch will be given to the listener. Any subsequent call will return the following batches.
     * <p>
     * Note that in some implementations it is possible that when there are no
     * results at all. {@link BatchedDataIterator#hasNext()} will return {@code true} the first time it is called but then a call
     * to this function returns an empty Collection to the listener.
     */
    public void next(ActionListener<E> listener) {
        if (!hasNext()) {
            listener.onFailure(new NoSuchElementException());
        }

        if (!isScrollInitialised) {
            ActionListener<SearchResponse> wrappedListener = ActionListener.wrap(
                searchResponse -> {
                    isScrollInitialised = true;
                    totalHits = searchResponse.getHits().getTotalHits().value;
                    scrollId = searchResponse.getScrollId();
                    mapHits(searchResponse, listener);
                },
                listener::onFailure
            );
            initScroll(wrappedListener);
        } else {
            ActionListener<SearchResponse> wrappedListener = ActionListener.wrap(
                searchResponse -> {
                    scrollId = searchResponse.getScrollId();
                    mapHits(searchResponse, listener);
                },
                listener::onFailure
            );
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId).scroll(CONTEXT_ALIVE_DURATION);
            ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
                ClientHelper.DATA_FRAME_ORIGIN,
                searchScrollRequest,
                wrappedListener,
                client::searchScroll);
        }
    }

    private void initScroll(ActionListener<SearchResponse> listener) {
        LOGGER.trace("ES API CALL: search index {}", index);

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        searchRequest.scroll(CONTEXT_ALIVE_DURATION);
        searchRequest.source(new SearchSourceBuilder()
            .fetchSource(getFetchSourceContext())
            .size(getBatchSize())
            .query(getQuery())
            .trackTotalHits(true)
            .sort(sortField(), sortOrder()));

        ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ClientHelper.DATA_FRAME_ORIGIN,
            searchRequest,
            listener,
            client::search);
    }

    private void mapHits(SearchResponse searchResponse, ActionListener<E> mappingListener) {
        E results = getCollection();

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            T mapped = map(hit);
            if (mapped != null) {
                results.add(mapped);
            }
        }
        count += hits.length;

        if (!hasNext() && scrollId != null) {
            ClearScrollRequest request = client.prepareClearScroll().setScrollIds(Collections.singletonList(scrollId)).request();
            ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
                ClientHelper.DATA_FRAME_ORIGIN,
                request,
                ActionListener.<ClearScrollResponse>wrap(
                    r -> mappingListener.onResponse(results),
                    mappingListener::onFailure
                ),
                client::clearScroll);
        } else {
            mappingListener.onResponse(results);
        }
    }

    /**
     * Get the query to use for the search
     * @return the search query
     */
    protected abstract QueryBuilder getQuery();

    /**
     * Maps the search hit to the document type
     * @param hit the search hit
     * @return The mapped document or {@code null} if the mapping failed
     */
    protected abstract T map(SearchHit hit);

    protected abstract E getCollection();

    protected abstract SortOrder sortOrder();

    protected abstract String sortField();

    /**
     * Should we fetch the source and what fields specifically.
     *
     * Defaults to all fields and true.
     */
    protected FetchSourceContext getFetchSourceContext() {
        return FetchSourceContext.FETCH_SOURCE;
    }

    protected int getBatchSize() {
        return BATCH_SIZE;
    }
}
