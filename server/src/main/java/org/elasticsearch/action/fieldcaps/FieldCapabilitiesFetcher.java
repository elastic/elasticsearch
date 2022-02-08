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
                return new FieldCapabilitiesIndexResponse(shardId.getIndexName(), Collections.emptyMap(), false);
            }

            Predicate<String> fieldPredicate = indicesService.getFieldFilter().apply(shardId.getIndexName());

            return retrieveFieldCaps(shardId.getIndexName(), searchExecutionContext, fieldPatterns, filters, fieldTypes, fieldPredicate);
        }
    }

    public static FieldCapabilitiesIndexResponse retrieveFieldCaps(
        String indexName,
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

        FieldCapsFilter filter = buildFilter(indexFieldfilter, filters, types);
        Map<String, IndexFieldCapabilities> responseMap = new HashMap<>();
        for (String field : fieldNames) {
            MappedFieldType ft = context.getFieldType(field);
            if (filter.matches(ft, context)) {
                IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(
                    field,
                    ft.familyTypeName(),
                    context.isMetadataField(field),
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
        return new FieldCapabilitiesIndexResponse(indexName, responseMap, true);
    }

    private static boolean checkIncludeParents(String[] filters) {
        for (String filter : filters) {
            if ("-parent".equals(filter)) {
                return false;
            }
            if ("parent".equals(filter)) {
                return true;
            }
        }
        return true;
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

    private interface FieldCapsFilter {
        boolean matches(MappedFieldType fieldType, SearchExecutionContext context);

        default FieldCapsFilter and(FieldCapsFilter other) {
            return (ft, context) -> matches(ft, context) && other.matches(ft, context);
        }
    }

    private static FieldCapsFilter buildFilter(Predicate<String> fieldFilter, String[] filters, String[] fieldTypes) {
        // security filters don't exclude metadata fields
        FieldCapsFilter fcf = (ft, c) -> fieldFilter.test(ft.name()) || c.isMetadataField(ft.name());
        if (fieldTypes.length > 0) {
            Set<String> acceptedTypes = Set.of(fieldTypes);
            fcf = fcf.and((ft, c) -> acceptedTypes.contains(ft.familyTypeName()));
        }
        for (String filter : filters) {
            if ("parent".equals(filter) || "-parent".equals(filter)) {
                continue;
            }
            FieldCapsFilter next = switch (filter) {
                case "+metadata" -> (ft, c) -> c.isMetadataField(ft.name());
                case "-metadata" -> (ft, c) -> c.isMetadataField(ft.name()) == false;
                case "-nested" -> (ft, c) -> c.nestedLookup().getNestedParent(ft.name()) == null;
                case "-multifield" -> (ft, c) -> c.isMultiField(ft.name()) == false;
                default -> throw new IllegalArgumentException("Unknown field caps filter [" + filter + "]");
            };
            fcf = fcf.and(next);
        }
        return fcf;
    }

}
