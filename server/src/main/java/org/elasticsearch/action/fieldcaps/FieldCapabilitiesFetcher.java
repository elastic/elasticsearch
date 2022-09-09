/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.cluster.metadata.MappingMetadata;
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
    private final Map<String, Map<String, IndexFieldCapabilities>> indexMappingHashToResponses = new HashMap<>();

    FieldCapabilitiesFetcher(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    FieldCapabilitiesIndexResponse fetch(
        ShardId shardId,
        String[] fieldPatterns,
        String[] filters,
        String[] fieldTypes,
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
                return new FieldCapabilitiesIndexResponse(shardId.getIndexName(), null, Collections.emptyMap(), false);
            }

            final MappingMetadata mapping = indexService.getMetadata().mapping();
            final String indexMappingHash = mapping != null ? mapping.getSha256() : null;
            if (indexMappingHash != null) {
                final Map<String, IndexFieldCapabilities> existing = indexMappingHashToResponses.get(indexMappingHash);
                if (existing != null) {
                    return new FieldCapabilitiesIndexResponse(shardId.getIndexName(), indexMappingHash, existing, true);
                }
            }

            Predicate<String> fieldPredicate = indicesService.getFieldFilter().apply(shardId.getIndexName());
            final Map<String, IndexFieldCapabilities> responseMap = retrieveFieldCaps(
                searchExecutionContext,
                fieldPatterns,
                filters,
                fieldTypes,
                fieldPredicate
            );
            if (indexMappingHash != null) {
                indexMappingHashToResponses.put(indexMappingHash, responseMap);
            }
            return new FieldCapabilitiesIndexResponse(shardId.getIndexName(), indexMappingHash, responseMap, true);
        }
    }

    static Map<String, IndexFieldCapabilities> retrieveFieldCaps(
        SearchExecutionContext context,
        String[] fieldPatterns,
        String[] filters,
        String[] types,
        Predicate<String> indexFieldfilter
    ) {

        Set<String> fieldNames = new HashSet<>();
        for (String pattern : fieldPatterns) {
            fieldNames.addAll(context.getMatchingFieldNames(pattern));
        }

        boolean includeParentObjects = checkIncludeParents(filters);

        Predicate<MappedFieldType> filter = buildFilter(indexFieldfilter, filters, types, context);
        boolean isTimeSeriesIndex = context.getIndexSettings().getTimestampBounds() != null;
        Map<String, IndexFieldCapabilities> responseMap = new HashMap<>();
        for (String field : fieldNames) {
            MappedFieldType ft = context.getFieldType(field);
            if (filter.test(ft)) {
                IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(
                    field,
                    // This is a nasty hack so that we expose aggregate_metric_double field,
                    // when the index is a time series index and the field is marked as metric.
                    // This code should be reverted once PR https://github.com/elastic/elasticsearch/pull/87849
                    // is merged.
                    isTimeSeriesIndex && ft.getMetricType() != null ? ft.typeName() : ft.familyTypeName(),
                    context.isMetadataField(field),
                    ft.isSearchable(),
                    ft.isAggregatable(),
                    isTimeSeriesIndex ? ft.isDimension() : false,
                    isTimeSeriesIndex ? ft.getMetricType() : null,
                    ft.meta()
                );
                responseMap.put(field, fieldCap);
            } else {
                continue;
            }

            // Check the ancestor of the field to find nested and object fields.
            // Runtime fields are excluded since they can override any path.
            // TODO find a way to do this that does not require an instanceof check
            if (ft instanceof RuntimeField == false && includeParentObjects) {
                int dotIndex = ft.name().lastIndexOf('.');
                while (dotIndex > -1) {
                    String parentField = ft.name().substring(0, dotIndex);
                    if (responseMap.containsKey(parentField)) {
                        // we added this path on another field already
                        break;
                    }
                    // checks if the parent field contains sub-fields
                    if (context.getFieldType(parentField) == null) {
                        // no field type, it must be an object field
                        String type = context.nestedLookup().getNestedMappers().get(parentField) != null ? "nested" : "object";
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
        return responseMap;
    }

    private static boolean checkIncludeParents(String[] filters) {
        for (String filter : filters) {
            if ("-parent".equals(filter)) {
                return false;
            }
        }
        return true;
    }

    private static boolean canMatchShard(
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

    private static Predicate<MappedFieldType> buildFilter(
        Predicate<String> fieldFilter,
        String[] filters,
        String[] fieldTypes,
        SearchExecutionContext context
    ) {
        // security filters don't exclude metadata fields
        Predicate<MappedFieldType> fcf = ft -> fieldFilter.test(ft.name()) || context.isMetadataField(ft.name());
        if (fieldTypes.length > 0) {
            Set<String> acceptedTypes = Set.of(fieldTypes);
            fcf = fcf.and(ft -> acceptedTypes.contains(ft.familyTypeName()));
        }
        for (String filter : filters) {
            if ("parent".equals(filter) || "-parent".equals(filter)) {
                continue;
            }
            Predicate<MappedFieldType> next = switch (filter) {
                case "+metadata" -> ft -> context.isMetadataField(ft.name());
                case "-metadata" -> ft -> context.isMetadataField(ft.name()) == false;
                case "-nested" -> ft -> context.nestedLookup().getNestedParent(ft.name()) == null;
                case "-multifield" -> ft -> context.isMultiField(ft.name()) == false;
                default -> throw new IllegalArgumentException("Unknown field caps filter [" + filter + "]");
            };
            fcf = fcf.and(next);
        }
        return fcf;
    }

}
