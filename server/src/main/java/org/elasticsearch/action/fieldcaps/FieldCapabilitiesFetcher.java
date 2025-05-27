/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.lucene.index.FieldInfos;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.PassThroughObjectMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Loads the mappings for an index and computes all {@link IndexFieldCapabilities}. This
 * helper class performs the core shard operation for the field capabilities action.
 */
class FieldCapabilitiesFetcher {
    private final IndicesService indicesService;
    private final boolean includeEmptyFields;
    private final Map<String, Map<String, IndexFieldCapabilities>> indexMappingHashToResponses = new HashMap<>();
    private static final boolean enableFieldHasValue = Booleans.parseBoolean(
        System.getProperty("es.field_caps_empty_fields_filter", Boolean.TRUE.toString())
    );

    FieldCapabilitiesFetcher(IndicesService indicesService, boolean includeEmptyFields) {
        this.indicesService = indicesService;
        this.includeEmptyFields = includeEmptyFields;
    }

    FieldCapabilitiesIndexResponse fetch(
        CancellableTask task,
        ShardId shardId,
        Predicate<String> fieldNameFilter,
        String[] filters,
        String[] fieldTypes,
        QueryBuilder indexFilter,
        long nowInMillis,
        Map<String, Object> runtimeFields
    ) throws IOException {
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.getId());
        final Engine.Searcher searcher;
        if (alwaysMatches(indexFilter)) {
            // no need to open a searcher if we aren't filtering, but make sure we are reading from an up-to-dated shard
            indexShard.readAllowed();
            searcher = null;
        } else {
            searcher = indexShard.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE);
        }
        try (searcher) {
            return doFetch(
                task,
                shardId,
                fieldNameFilter,
                filters,
                fieldTypes,
                indexFilter,
                nowInMillis,
                runtimeFields,
                indexService,
                searcher
            );
        }
    }

    private FieldCapabilitiesIndexResponse doFetch(
        CancellableTask task,
        ShardId shardId,
        Predicate<String> fieldNameFilter,
        String[] filters,
        String[] fieldTypes,
        QueryBuilder indexFilter,
        long nowInMillis,
        Map<String, Object> runtimeFields,
        IndexService indexService,
        @Nullable Engine.Searcher searcher
    ) throws IOException {
        final SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            shardId.id(),
            0,
            searcher,
            () -> nowInMillis,
            null,
            runtimeFields
        );
        var indexMode = searchExecutionContext.getIndexSettings().getMode();
        if (searcher != null && canMatchShard(shardId, indexFilter, nowInMillis, searchExecutionContext) == false) {
            return new FieldCapabilitiesIndexResponse(shardId.getIndexName(), null, Collections.emptyMap(), false, indexMode);
        }

        final MappingMetadata mapping = indexService.getMetadata().mapping();
        String indexMappingHash;
        if (includeEmptyFields || enableFieldHasValue == false) {
            indexMappingHash = mapping != null ? mapping.getSha256() + indexMode : null;
        } else {
            // even if the mapping is the same if we return only fields with values we need
            // to make sure that we consider all the shard-mappings pair, that is why we
            // calculate a different hash for this particular case.
            final String shardUuid = indexService.getShard(shardId.getId()).getShardUuid();
            indexMappingHash = mapping == null ? shardUuid : shardUuid + mapping.getSha256();
        }
        FieldPredicate fieldPredicate = indicesService.getFieldFilter().apply(shardId.getIndexName());
        if (indexMappingHash != null) {
            indexMappingHash = fieldPredicate.modifyHash(indexMappingHash);
            final Map<String, IndexFieldCapabilities> existing = indexMappingHashToResponses.get(indexMappingHash);
            if (existing != null) {
                return new FieldCapabilitiesIndexResponse(shardId.getIndexName(), indexMappingHash, existing, true, indexMode);
            }
        }
        task.ensureNotCancelled();
        final Map<String, IndexFieldCapabilities> responseMap = retrieveFieldCaps(
            searchExecutionContext,
            fieldNameFilter,
            filters,
            fieldTypes,
            fieldPredicate,
            indicesService.getShardOrNull(shardId),
            includeEmptyFields
        );
        if (indexMappingHash != null) {
            indexMappingHashToResponses.put(indexMappingHash, responseMap);
        }
        return new FieldCapabilitiesIndexResponse(shardId.getIndexName(), indexMappingHash, responseMap, true, indexMode);
    }

    static Map<String, IndexFieldCapabilities> retrieveFieldCaps(
        SearchExecutionContext context,
        Predicate<String> fieldNameFilter,
        String[] filters,
        String[] types,
        FieldPredicate fieldPredicate,
        IndexShard indexShard,
        boolean includeEmptyFields
    ) {
        boolean includeParentObjects = checkIncludeParents(filters);
        Predicate<MappedFieldType> filter = buildFilter(filters, types, context);
        boolean isTimeSeriesIndex = context.getIndexSettings().getTimestampBounds() != null;
        includeEmptyFields = includeEmptyFields || enableFieldHasValue == false;
        Map<String, IndexFieldCapabilities> responseMap = new HashMap<>();

        // populate with all types of mappers
        RootObjectMapper rootObjectMapper = context.getMappingLookup().getMapping().getRoot();
        List<Mapper> allMappers = new ArrayList<>();
        allMappers.addAll(rootObjectMapper.getSourceFields());
        allMappers.addAll(context.getMetadataFields());

        for (Mapper mapper : allMappers) {
            if (mapper instanceof PassThroughObjectMapper) {
                // Handles PassThroughObjectMapper by processing its immediate child fields.
                for (Mapper childMapper : mapper.getSourceFields()) {
                    addFieldToFieldCaps(
                        childMapper.leafName(),
                        context.getFieldType(childMapper.fullPath()),
                        fieldNameFilter,
                        fieldPredicate,
                        filter,
                        context,
                        indexShard.getFieldInfos(),
                        includeEmptyFields,
                        isTimeSeriesIndex,
                        includeParentObjects,
                        responseMap
                    );
                }
            } else {
                addFieldToFieldCaps(
                    mapper.fullPath(),
                    context.getFieldType(mapper.fullPath()),
                    fieldNameFilter,
                    fieldPredicate,
                    filter,
                    context,
                    indexShard.getFieldInfos(),
                    includeEmptyFields,
                    isTimeSeriesIndex,
                    includeParentObjects,
                    responseMap
                );
            }
        }

        // populate runtime fields
        Collection<RuntimeField> runtimeFields = rootObjectMapper.runtimeFields();
        Map<String, MappedFieldType> runtimeFieldTypes = new HashMap<>(RuntimeField.collectFieldTypes(runtimeFields));
        runtimeFieldTypes.putAll(context.getRuntimeMappings());

        for (Map.Entry<String, MappedFieldType> entry : runtimeFieldTypes.entrySet()) {
            addFieldToFieldCaps(
                entry.getKey(),
                entry.getValue(),
                fieldNameFilter,
                fieldPredicate,
                filter,
                context,
                indexShard.getFieldInfos(),
                includeEmptyFields,
                isTimeSeriesIndex,
                includeParentObjects,
                responseMap
            );
        }

        return responseMap;
    }

    private static void addFieldToFieldCaps(
        String fieldName,
        MappedFieldType fieldType,
        Predicate<String> fieldNameFilter,
        FieldPredicate fieldPredicate,
        Predicate<MappedFieldType> filter,
        SearchExecutionContext context,
        FieldInfos fieldInfos,
        boolean includeEmptyFields,
        boolean isTimeSeriesIndex,
        boolean includeParentObjects,
        Map<String, IndexFieldCapabilities> responseMap
    ) {
        if (fieldNameFilter.test(fieldName) == false || fieldType == null) {
            return;
        }

        if ((includeEmptyFields || fieldType.fieldHasValue(fieldInfos))
            && (fieldPredicate.test(fieldType.name()) || context.isMetadataField(fieldType.name()))
            && (filter == null || filter.test(fieldType))) {

            IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(
                fieldName,
                fieldType.familyTypeName(),
                context.isMetadataField(fieldName),
                fieldType.isSearchable(),
                fieldType.isAggregatable(),
                isTimeSeriesIndex ? fieldType.isDimension() : false,
                isTimeSeriesIndex ? fieldType.getMetricType() : null,
                fieldType.meta()
            );
            responseMap.put(fieldName, fieldCap);

            // Add parent object/nested fields if needed
            // Runtime fields are excluded since they can override any path
            if (fieldType instanceof RuntimeField == false && includeParentObjects) {
                addParentFieldsToFieldCaps(fieldType.name(), context, responseMap);
            }
        }
    }

    private static void addParentFieldsToFieldCaps(
        String fieldName,
        SearchExecutionContext context,
        Map<String, IndexFieldCapabilities> responseMap
    ) {
        int dotIndex = fieldName.lastIndexOf('.');
        while (dotIndex > -1) {
            String parentField = fieldName.substring(0, dotIndex);

            if (responseMap.containsKey(parentField)) {
                // We already added this parent field
                break;
            }

            // Check if the parent field contains sub-fields
            if (context.getFieldType(parentField) == null) {
                // No field type, it must be an object field
                String type = context.nestedLookup().getNestedMappers().get(parentField) != null ? "nested" : "object";

                IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(parentField, type, false, false, false, false, null, Map.of());
                responseMap.put(parentField, fieldCap);
            }

            dotIndex = parentField.lastIndexOf('.');
        }
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
        assert alwaysMatches(indexFilter) == false : "should not be called for always matching [" + indexFilter + "]";
        assert nowInMillis != 0L;
        ShardSearchRequest searchRequest = new ShardSearchRequest(shardId, nowInMillis, AliasFilter.EMPTY);
        searchRequest.source(new SearchSourceBuilder().query(indexFilter));
        return SearchService.queryStillMatchesAfterRewrite(searchRequest, searchExecutionContext);
    }

    private static boolean alwaysMatches(QueryBuilder indexFilter) {
        return indexFilter == null || indexFilter instanceof MatchAllQueryBuilder;
    }

    private static Predicate<MappedFieldType> buildFilter(String[] filters, String[] fieldTypes, SearchExecutionContext context) {
        // security filters don't exclude metadata fields
        Predicate<MappedFieldType> fcf = null;
        if (fieldTypes.length > 0) {
            Set<String> acceptedTypes = Set.of(fieldTypes);
            fcf = ft -> acceptedTypes.contains(ft.familyTypeName());
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
            fcf = fcf == null ? next : fcf.and(next);
        }
        return fcf;
    }

}
