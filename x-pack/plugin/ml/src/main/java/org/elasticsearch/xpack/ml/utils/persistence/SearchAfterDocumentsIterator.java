/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.Objects;

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
 */
public abstract class SearchAfterDocumentsIterator<T> implements BatchedIterator<T> {

    private static final int BATCH_SIZE = 10_000;

    private final OriginSettingClient client;
    private final String index;
    private volatile long count;
    private volatile long totalHits;

    protected SearchAfterDocumentsIterator(OriginSettingClient client, String index) {
        this.client = Objects.requireNonNull(client);
        this.index = Objects.requireNonNull(index);
        this.totalHits = -1;
        this.count = 0;
    }

    /**
     * Returns {@code true} if the iteration has more elements or
     * no searches have been been run and it is unknown if there is a next.
     *
     * @return {@code true} if the iteration has more elements or the first
     * search has not been run
     */
    @Override
    public boolean hasNext() {
        return count != totalHits;
    }

    /**
     * The first time next() is called, the search will be performed and the first
     * batch will be returned. Any subsequent call will return the following batches.
     * <p>
     * Note that in some implementations it is possible that when there are no
     * results at all, the first time this method is called an empty {@code Deque} is returned.
     *
     * @return a {@code Deque} with the next batch of documents
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Deque<T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        SearchResponse searchResponse;
        if (totalHits == -1) {
            searchResponse = initSearch();
        } else {
            searchResponse = doSearch(searchAfterFields());
        }
        return mapHits(searchResponse);
    }

    private SearchResponse initSearch() {
        SearchResponse searchResponse = doSearch(null);
        totalHits = searchResponse.getHits().getTotalHits().value;
        return searchResponse;
    }

    private SearchResponse doSearch(Object [] searchAfterValues) {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));
        SearchSourceBuilder sourceBuilder = (new SearchSourceBuilder()
            .size(BATCH_SIZE)
            .query(getQuery())
            .fetchSource(shouldFetchSource())
            .trackTotalHits(true)
            .sort(sortField()));

        if (searchAfterValues != null) {
            sourceBuilder.searchAfter(searchAfterValues);
        }

        searchRequest.source(sourceBuilder);
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
        count += hits.length;

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
}
