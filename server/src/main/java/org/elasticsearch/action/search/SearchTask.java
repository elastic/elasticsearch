/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Task storing information about a currently running {@link SearchRequest}.
 */
public class SearchTask extends CancellableTask {
    // generating description in a lazy way since source can be quite big
    private final Supplier<String> descriptionSupplier;
    private SearchProgressListener progressListener = SearchProgressListener.NOOP;

    public SearchTask(
        long id,
        String type,
        String action,
        Supplier<String> descriptionSupplier,
        TaskId parentTaskId,
        Map<String, String> headers
    ) {
        super(id, type, action, null, parentTaskId, headers);
        this.descriptionSupplier = descriptionSupplier;
    }

    @Override
    public final String getDescription() {
        return descriptionSupplier.get();
    }

    /**
     * Attach a {@link SearchProgressListener} to this task.
     */
    public final void setProgressListener(SearchProgressListener progressListener) {
        this.progressListener = progressListener;
    }

    /**
     * Return the {@link SearchProgressListener} attached to this task.
     */
    public final SearchProgressListener getProgressListener() {
        return progressListener;
    }

    /**
     * @return a Supplier that provides SearchResponseMerger. Needed for CCS minimize_roundtrips=true.
     */
    protected Supplier<SearchResponseMerger> getSearchResponseMergerSupplier(
        SearchSourceBuilder source,
        TransportSearchAction.SearchTimeProvider timeProvider,
        AggregationReduceContext.Builder aggReduceContextBuilder
    ) {
        return () -> createSearchResponseMerger(source, timeProvider, aggReduceContextBuilder);
    }

    protected static SearchResponseMerger createSearchResponseMerger(
        SearchSourceBuilder source,
        TransportSearchAction.SearchTimeProvider timeProvider,
        AggregationReduceContext.Builder aggReduceContextBuilder
    ) {
        final int from;
        final int size;
        final int trackTotalHitsUpTo;
        if (source == null) {
            from = SearchService.DEFAULT_FROM;
            size = SearchService.DEFAULT_SIZE;
            trackTotalHitsUpTo = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;
        } else {
            from = source.from() == -1 ? SearchService.DEFAULT_FROM : source.from();
            size = source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size();
            trackTotalHitsUpTo = source.trackTotalHitsUpTo() == null
                ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : source.trackTotalHitsUpTo();
            // here we modify the original source so we can re-use it by setting it to each outgoing search request
            source.from(0);
            source.size(from + size);
        }
        return new SearchResponseMerger(from, size, trackTotalHitsUpTo, timeProvider, aggReduceContextBuilder);
    }
}
