/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Loads the mappings for an index and computes all {@link IndexFieldCapabilities}. This
 * helper class performs the core shard operation for the field capabilities action.
 */
class FieldCapabilitiesFetcher {
    private final IndicesService indicesService;

    FieldCapabilitiesFetcher(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    FieldCapabilitiesIndexResponse fetch(
        ShardId shardId,
        String[] fieldPatterns,
        QueryBuilder indexFilter,
        long nowInMillis,
        Map<String, Object> runtimeFields
    ) throws IOException {
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.getId());
        try (Engine.Searcher searcher = indexShard.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE)) {

            final SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
                shardId.id(),
                0,
                searcher,
                () -> nowInMillis,
                null,
                runtimeFields
            );

            if (canMatchShard(shardId, indexFilter, nowInMillis, searchExecutionContext) == false) {
                return new FieldCapabilitiesIndexResponse(shardId.getIndexName(), Collections.emptyMap(), false);
            }

            Set<String> fieldNames = new HashSet<>();
            for (String pattern : fieldPatterns) {
                fieldNames.addAll(searchExecutionContext.getMatchingFieldNames(pattern));
            }

            Predicate<String> fieldPredicate = indicesService.getFieldFilter().apply(shardId.getIndexName());
            Map<String, IndexFieldCapabilities> responseMap = new HashMap<>();
            for (String field : fieldNames) {
                MappedFieldType ft = searchExecutionContext.getFieldType(field);
                boolean isMetadataField = searchExecutionContext.isMetadataField(field);
                if (isMetadataField || fieldPredicate.test(ft.name())) {
                    IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(
                        field,
                        ft.familyTypeName(),
                        isMetadataField,
                        ft.isSearchable(),
                        ft.isAggregatable(),
                        ft.isDimension(),
                        ft.getMetricType(),
                        ft.meta()
                    );
                    responseMap.put(field, fieldCap);
                } else {
                    continue;
                }

                // Check the ancestor of the field to find nested and object fields.
                // Runtime fields are excluded since they can override any path.
                // TODO find a way to do this that does not require an instanceof check
                if (ft instanceof RuntimeField == false) {
                    int dotIndex = ft.name().lastIndexOf('.');
                    while (dotIndex > -1) {
                        String parentField = ft.name().substring(0, dotIndex);
                        if (responseMap.containsKey(parentField)) {
                            // we added this path on another field already
                            break;
                        }
                        // checks if the parent field contains sub-fields
                        if (searchExecutionContext.getFieldType(parentField) == null) {
                            // no field type, it must be an object field
                            String type = searchExecutionContext.nestedLookup().getNestedMappers().get(parentField) != null
                                ? "nested"
                                : "object";
                            IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(
                                parentField,
                                type,
                                false,
                                false,
                                false,
                                false,
                                null,
                                Collections.emptyMap()
                            );
                            responseMap.put(parentField, fieldCap);
                        }
                        dotIndex = parentField.lastIndexOf('.');
                    }
                }
            }
            return new FieldCapabilitiesIndexResponse(shardId.getIndexName(), responseMap, true);
        }
    }

    private boolean canMatchShard(
        ShardId shardId,
        QueryBuilder indexFilter,
        long nowInMillis,
        SearchExecutionContext searchExecutionContext
    ) throws IOException {
        if (indexFilter == null || indexFilter instanceof MatchAllQueryBuilder) {
            return true;
        }
        assert nowInMillis != 0L;
        ShardSearchRequest searchRequest = new ShardSearchRequest(shardId, nowInMillis, AliasFilter.EMPTY);
        searchRequest.source(new SearchSourceBuilder().query(indexFilter));
        return SearchService.queryStillMatchesAfterRewrite(searchRequest, searchExecutionContext);
    }

}
