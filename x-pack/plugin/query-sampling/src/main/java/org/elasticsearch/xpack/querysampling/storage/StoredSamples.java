/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Reads sampled queries back from {@link QuerySamplingIndex}.
 */
public final class StoredSamples {

    private static final Logger logger = LogManager.getLogger(StoredSamples.class);

    /**
     * @param unreadable documents that matched but could not be parsed
     */
    public record Read(List<StoredSample> samples, int unreadable) {}

    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;
    private final NamedXContentRegistry registry;

    public StoredSamples(BiConsumer<SearchRequest, ActionListener<SearchResponse>> search, NamedXContentRegistry registry) {
        this.search = search;
        this.registry = registry;
    }

    /** A missing index, because nothing was ever sampled, reads as empty. */
    public void read(QueryBuilder query, String sortField, SortOrder order, int max, ActionListener<Read> listener) {
        SearchRequest request = new SearchRequest(QuerySamplingIndex.NAME).source(
            new SearchSourceBuilder().query(query).sort(sortField, order).size(max)
        );
        search.accept(request, ActionListener.wrap(response -> listener.onResponse(read(response)), e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                listener.onResponse(new Read(List.of(), 0));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private Read read(SearchResponse response) {
        List<StoredSample> samples = new ArrayList<>();
        int unreadable = 0;
        for (SearchHit hit : response.getHits().getHits()) {
            try {
                samples.add(SampleRecord.parse(hit.getSourceAsMap(), registry));
            } catch (Exception e) {
                unreadable++;
                logger.debug("failed to read the sampled query [{}]", hit.getId(), e);
            }
        }
        return new Read(samples, unreadable);
    }
}
