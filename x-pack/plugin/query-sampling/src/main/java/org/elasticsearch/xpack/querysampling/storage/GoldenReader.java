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
import org.elasticsearch.index.query.QueryBuilders;
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
 * Reads versions of the golden dataset back from {@link GoldenIndex}.
 */
public final class GoldenReader {

    private static final Logger logger = LogManager.getLogger(GoldenReader.class);

    public record Read(List<StoredSample> records, int unreadable) {}

    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;
    private final NamedXContentRegistry registry;

    public GoldenReader(BiConsumer<SearchRequest, ActionListener<SearchResponse>> search, NamedXContentRegistry registry) {
        this.search = search;
        this.registry = registry;
    }

    /** The latest completed version, 0 if there is none. */
    public void latestCompleted(ActionListener<Long> listener) {
        SearchRequest request = new SearchRequest(GoldenIndex.NAME).source(
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("kind", GoldenIndex.VERSION))
                    .filter(QueryBuilders.termQuery("completed", true))
            ).sort("dataset_version", SortOrder.DESC).size(1)
        );
        search.accept(request, ActionListener.wrap(response -> {
            SearchHit[] hits = response.getHits().getHits();
            listener.onResponse(hits.length == 0 ? 0L : ((Number) hits[0].getSourceAsMap().get("dataset_version")).longValue());
        }, e -> noIndexIs(0L, e, listener)));
    }

    /** Reads up to {@code max} queries of a version in fingerprint order; a missing version or index has none. */
    public void read(long version, int max, ActionListener<Read> listener) {
        SearchRequest request = new SearchRequest(GoldenIndex.NAME).source(
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("kind", GoldenIndex.RECORD))
                    .filter(QueryBuilders.termQuery("dataset_version", version))
            ).sort("fingerprint", SortOrder.ASC).size(max)
        );
        search.accept(request, ActionListener.wrap(response -> listener.onResponse(read(response)), e -> {
            noIndexIs(new Read(List.of(), 0), e, listener);
        }));
    }

    private Read read(SearchResponse response) {
        List<StoredSample> records = new ArrayList<>();
        int unreadable = 0;
        for (SearchHit hit : response.getHits().getHits()) {
            try {
                records.add(GoldenRecord.parse(hit.getSourceAsMap(), registry));
            } catch (Exception e) {
                unreadable++;
                logger.debug("failed to read the golden record [{}]", hit.getId(), e);
            }
        }
        return new Read(records, unreadable);
    }

    // a missing index just means nothing was ever promoted
    private static <T> void noIndexIs(T nothing, Exception e, ActionListener<T> listener) {
        if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
            listener.onResponse(nothing);
        } else {
            listener.onFailure(e);
        }
    }
}
