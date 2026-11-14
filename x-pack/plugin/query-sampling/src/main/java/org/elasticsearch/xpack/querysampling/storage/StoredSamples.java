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
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Reads sampled queries back from {@link QuerySamplingIndex}.
 */
public final class StoredSamples {

    private static final Logger logger = LogManager.getLogger(StoredSamples.class);

    /**
     * @param unreadableIds the documents that matched but could not be parsed
     */
    public record Read(List<StoredSample> samples, List<String> unreadableIds) {
        public int unreadable() {
            return unreadableIds.size();
        }
    }

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
        read(search, request, source -> SampleRecord.parse(source, registry), listener);
    }

    /** Shared with {@link GoldenReader}: a document that cannot be parsed is counted and skipped, a missing index reads as empty. */
    static void read(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> search,
        SearchRequest request,
        CheckedFunction<Map<String, Object>, StoredSample, IOException> parse,
        ActionListener<Read> listener
    ) {
        search.accept(request, ActionListener.wrap(response -> {
            List<StoredSample> samples = new ArrayList<>();
            List<String> unreadable = new ArrayList<>();
            Exception first = null;
            for (SearchHit hit : response.getHits().getHits()) {
                try {
                    samples.add(parse.apply(hit.getSourceAsMap()));
                } catch (Exception e) {
                    unreadable.add(hit.getId());
                    first = first == null ? e : first;
                }
            }
            if (unreadable.isEmpty() == false) {
                logger.warn("[{}] stored queries could not be read, for example [{}]", unreadable.size(), unreadable.get(0), first);
            }
            listener.onResponse(new Read(samples, unreadable));
        }, e -> noIndexIs(new Read(List.of(), List.of()), e, listener)));
    }

    /** A missing index just means nothing was ever stored. */
    static <T> void noIndexIs(T nothing, Exception e, ActionListener<T> listener) {
        if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
            listener.onResponse(nothing);
        } else {
            listener.onFailure(e);
        }
    }
}
