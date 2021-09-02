/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.transform.transforms.Function;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * {@link Function.ChangeCollector} implementation for the latest function.
 */
class LatestChangeCollector implements Function.ChangeCollector {

    private final String synchronizationField;

    LatestChangeCollector(String synchronizationField) {
        this.synchronizationField = Objects.requireNonNull(synchronizationField);
    }

    @Override
    public SearchSourceBuilder buildChangesQuery(SearchSourceBuilder searchSourceBuilder, Map<String, Object> position, int pageSize) {
        // This method is never called because "queryForChanges()" method returns "false".
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> processSearchResponse(SearchResponse searchResponse) {
        // This method is never called because "queryForChanges()" method returns "false".
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryBuilder buildFilterQuery(TransformCheckpoint lastCheckpoint, TransformCheckpoint nextCheckpoint) {
        // We are only interested in documents that were created in the timeline of the current checkpoint.
        // Older documents cannot influence the transform results as we require the sort field values to change monotonically over time.
        return QueryBuilders.rangeQuery(synchronizationField)
            .gte(lastCheckpoint.getTimeUpperBound())
            .lt(nextCheckpoint.getTimeUpperBound())
            .format("epoch_millis");
    }

    @Override
    public Collection<String> getIndicesToQuery(TransformCheckpoint lastCheckpoint, TransformCheckpoint nextCheckpoint) {
        // we can shortcut here, only the changed indices are of interest
        return TransformCheckpoint.getChangedIndices(lastCheckpoint, nextCheckpoint);
    }

    @Override
    public void clear() {
        // This object is stateless so there is no internal state to clear
    }

    @Override
    public boolean isOptimized() {
        // Change collection is optimized as we never process a document more than once.
        return true;
    }

    @Override
    public boolean queryForChanges() {
        // Calculating latest does not require elaborate change detection mechanism that is used in pivot.
        return false;
    }
}
