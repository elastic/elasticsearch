/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.utils.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 *
 */
public abstract class SearchAfterBatchedDocumentsIterator<T> {

    private static final Logger logger = LogManager.getLogger(SearchAfterBatchedDocumentsIterator.class);

    private static final int BATCH_SIZE = 10000;

    private final OriginSettingClient client;
    private final String index;
    private volatile long count;
    private volatile long totalHits;

    protected SearchAfterBatchedDocumentsIterator(OriginSettingClient client, String index) {
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
    public Deque<T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        SearchResponse searchResponse;
        if (totalHits == -1) {
            searchResponse = initSearch();
        } else {
            searchResponse = client.searchScroll(searchScrollRequest).actionGet();
        }
        return mapHits(searchResponse);
    }

    private SearchResponse initSearch() {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));
        searchRequest.source(new SearchSourceBuilder()
            .size(BATCH_SIZE)
            .query(getQuery())
            .fetchSource(shouldFetchSource())
            .trackTotalHits(true)
            .searchAfter()
            .sort(sortField()));

        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        totalHits = searchResponse.getHits().getTotalHits().value;
        return searchResponse;
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
}
