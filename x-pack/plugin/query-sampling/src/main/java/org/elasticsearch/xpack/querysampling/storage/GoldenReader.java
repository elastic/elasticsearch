/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.function.BiConsumer;

/**
 * Reads versions of the golden dataset back from {@link GoldenIndex}.
 */
public final class GoldenReader {

    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;
    private final NamedXContentRegistry registry;

    public GoldenReader(BiConsumer<SearchRequest, ActionListener<SearchResponse>> search, NamedXContentRegistry registry) {
        this.search = search;
        this.registry = registry;
    }

    /** The latest completed version, 0 if there is none. */
    public void latestCompleted(ActionListener<Long> listener) {
        latest(
            search,
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("kind", GoldenIndex.VERSION))
                .filter(QueryBuilders.termQuery("completed", true)),
            listener
        );
    }

    /** The latest version matching the query, 0 if there is none or no index. */
    static void latest(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> search,
        QueryBuilder query,
        ActionListener<Long> listener
    ) {
        SearchRequest request = new SearchRequest(GoldenIndex.NAME).source(
            new SearchSourceBuilder().query(query).sort("dataset_version", SortOrder.DESC).size(1)
        );
        search.accept(request, ActionListener.wrap(response -> {
            SearchHit[] hits = response.getHits().getHits();
            listener.onResponse(hits.length == 0 ? 0L : ((Number) hits[0].getSourceAsMap().get("dataset_version")).longValue());
        }, e -> StoredSamples.noIndexIs(0L, e, listener)));
    }

    /** Reads up to {@code max} queries of a version in fingerprint order; a missing version or index has none. */
    public void read(long version, int max, ActionListener<StoredSamples.Read> listener) {
        SearchRequest request = new SearchRequest(GoldenIndex.NAME).source(
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("kind", GoldenIndex.RECORD))
                    .filter(QueryBuilders.termQuery("dataset_version", version))
            ).sort("fingerprint", SortOrder.ASC).size(max)
        );
        StoredSamples.read(search, request, source -> GoldenRecord.parse(source, registry), listener);
    }
}
