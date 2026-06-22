/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.Set;

/**
 * Schema for the index-backed abstractions (concrete indices, aliases, data streams) — produced by the
 * field-caps fetch behind {@link IndexResolver}. This provider forwards verbatim; the shard-level fetch and
 * the ES|QL-side mapping merge stay exactly where they are.
 */
final class IndexSchemaProvider {

    private final IndexResolver indexResolver;

    IndexSchemaProvider(IndexResolver indexResolver) {
        this.indexResolver = indexResolver;
    }

    void resolveMainIndicesVersioned(
        String indexPattern,
        Set<String> fieldNames,
        QueryBuilder requestFilter,
        boolean includeAllDimensions,
        TransportVersion minimumVersion,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        boolean hasTimeSeriesAggregation,
        boolean trackUnmappedFieldIndices,
        IndicesExpressionGrouper indicesExpressionGrouper,
        ActionListener<Versioned<IndexResolution>> listener
    ) {
        indexResolver.resolveMainIndicesVersioned(
            indexPattern,
            fieldNames,
            requestFilter,
            includeAllDimensions,
            minimumVersion,
            useAggregateMetricDoubleWhenNotSupported,
            useDenseVectorWhenNotSupported,
            hasTimeSeriesAggregation,
            trackUnmappedFieldIndices,
            indicesExpressionGrouper,
            listener
        );
    }

    void resolveFlatIndicesVersioned(
        boolean lenient,
        String indexPattern,
        String projectRouting,
        Set<String> fieldNames,
        QueryBuilder requestFilter,
        boolean includeAllDimensions,
        TransportVersion minimumVersion,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        boolean hasTimeSeriesAggregation,
        boolean trackUnmappedFieldIndices,
        ActionListener<Versioned<IndexResolution>> listener
    ) {
        indexResolver.resolveFlatIndicesVersioned(
            lenient,
            indexPattern,
            projectRouting,
            fieldNames,
            requestFilter,
            includeAllDimensions,
            minimumVersion,
            useAggregateMetricDoubleWhenNotSupported,
            useDenseVectorWhenNotSupported,
            hasTimeSeriesAggregation,
            trackUnmappedFieldIndices,
            listener
        );
    }

    void resolveLookupIndices(
        String indexPattern,
        Set<String> fieldNames,
        TransportVersion minimumVersion,
        ActionListener<IndexResolution> listener
    ) {
        indexResolver.resolveLookupIndices(indexPattern, fieldNames, minimumVersion, listener);
    }
}
