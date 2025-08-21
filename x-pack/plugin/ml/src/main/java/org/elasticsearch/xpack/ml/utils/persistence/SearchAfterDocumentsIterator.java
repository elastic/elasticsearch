/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An iterator useful to fetch a large number of documents of type T
 * and iterate through them in batches of 10,000.
 *
 * In terms of functionality this is very similar to {@link BatchedDocumentsIterator}
 * the difference being that this uses search after rather than scroll.
 *
 * Search after has the advantage that the scroll context does not have to be kept
 * alive so if processing each batch takes a long time search after should be
 * preferred to scroll.
 *
 * Documents in the index may be deleted or updated between search after calls
 * so it is possible that the total hits can change. For this reason the hit
 * count isn't a reliable indicator of progress and the iterator will judge that
 * it has reached the end of the search only when less than {@value #BATCH_SIZE}
 * hits are returned.
 */
public abstract class SearchAfterDocumentsIterator<T> implements BatchedIterator<T> {

    private static final int BATCH_SIZE = 10_000;

    private final OriginSettingClient client;
    private final String index;
    private final boolean trackTotalHits;
    private final AtomicLong totalHits = new AtomicLong();
    private final AtomicBoolean lastSearchReturnedResults;
    private int batchSize = BATCH_SIZE;

    protected SearchAfterDocumentsIterator(OriginSettingClient client, String index) {
        this(client, index, false);
    }

    /**
     * Constructs an iterator that searches docs using search after
     *
     * @param client the client
     * @param index the index
     * @param trackTotalHits whether to track total hits. Note this is only done in the first search
     *                       and the result will only be accurate if the index is not changed between searches.
     */
    protected SearchAfterDocumentsIterator(OriginSettingClient client, String index, boolean trackTotalHits) {
        this.client = Objects.requireNonNull(client);
        this.index = Objects.requireNonNull(index);
        this.trackTotalHits = trackTotalHits;
        this.lastSearchReturnedResults = new AtomicBoolean(true);
    }

    /**
     * Returns {@code true} if the iteration has more elements or
     * no searches have been been run and it is unknown if there is a next.
     *
     * Because the index may change between search after calls it is not possible
     * to know how many results will be returned until all have been seen.
     * For this reason is it possible {@code hasNext} will return true even
     * if the next search returns 0 search hits. In that case {@link #next()}
     * will return an empty collection.
     *
     * @return {@code true} if the iteration has more elements or the first
     * search has not been run
     */
    @Override
    public boolean hasNext() {
        return lastSearchReturnedResults.get();
    }

    /**
     * The first time next() is called, the search will be performed and the first
     * batch will be returned. Subsequent calls will return the following batches.
     *
     * Note it is possible that when there are no results at all, the first time
     * this method is called an empty {@code Deque} is returned.
     *
     * @return a {@code Deque} with the next batch of documents
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Deque<T> next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        SearchResponse searchResponse = doSearch(searchAfterFields());
        try {
            if (trackTotalHits && totalHits.get() == 0) {
                totalHits.set(searchResponse.getHits().getTotalHits().value());
            }
            return mapHits(searchResponse);
        } finally {
            searchResponse.decRef();
        }
    }

    private SearchResponse doSearch(Object[] searchAfterValues) {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));
        SearchSourceBuilder sourceBuilder = (new SearchSourceBuilder().size(batchSize)
            .query(getQuery())
            .fetchSource(shouldFetchSource())
            .sort(sortField()));

        if (trackTotalHits && totalHits.get() == 0L) {
            sourceBuilder.trackTotalHits(true);
        }

        if (searchAfterValues != null) {
            sourceBuilder.searchAfter(searchAfterValues);
        }

        for (Map.Entry<String, String> docValueFieldAndFormat : docValueFieldAndFormatPairs().entrySet()) {
            sourceBuilder.docValueField(docValueFieldAndFormat.getKey(), docValueFieldAndFormat.getValue());
        }

        searchRequest.source(sourceBuilder);
        return executeSearchRequest(searchRequest);
    }

    protected Map<String, String> docValueFieldAndFormatPairs() {
        return Collections.emptyMap();
    }

    protected SearchResponse executeSearchRequest(SearchRequest searchRequest) {
        return client.search(searchRequest).actionGet();
    }

    private Deque<T> mapHits(SearchResponse searchResponse) {
        Deque<T> results = new ArrayDeque<>();

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            T mapped = map(hit);
            if (mapped != null) {
                results.add(mapped);
            }
        }

        // fewer hits than we requested, this is the end of the search
        if (hits.length < batchSize) {
            lastSearchReturnedResults.set(false);
        }

        if (hits.length > 0) {
            extractSearchAfterFields(hits[hits.length - 1]);
        }

        return results;
    }

    /**
     * Should fetch source? Defaults to {@code true}
     * @return whether the source should be fetched
     */
    protected boolean shouldFetchSource() {
        return true;
    }

    /**
     * Get the query to use for the search
     * @return the search query
     */
    protected abstract QueryBuilder getQuery();

    /**
     * The field to sort results on. This should have a unique value per document
     * for search after.
     * @return The sort field
     */
    protected abstract FieldSortBuilder sortField();

    /**
     * Maps the search hit to the document type
     * @param hit
     *            the search hit
     * @return The mapped document or {@code null} if the mapping failed
     */
    protected abstract T map(SearchHit hit);

    /**
     * The field to be used in the next search
      * @return The search after fields
     */
    protected abstract Object[] searchAfterFields();

    /**
     * Extract the fields used in search after from the search hit.
     * The values are stashed and later returned by {@link #searchAfterFields()}
     * @param lastSearchHit The last search hit in the previous search response
     */
    protected abstract void extractSearchAfterFields(SearchHit lastSearchHit);

    // for testing
    void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    protected Client client() {
        return client;
    }

    public long getTotalHits() {
        if (trackTotalHits == false) {
            throw new IllegalStateException("cannot return total hits because tracking was not enabled");
        }
        return totalHits.get();
    }
}
