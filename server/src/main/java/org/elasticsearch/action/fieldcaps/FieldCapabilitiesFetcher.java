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
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
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
    private final IndexFieldCapabilities.Deduplicator fieldDeduplicator;

    FieldCapabilitiesFetcher(IndicesService indicesService) {
        this.indicesService = indicesService;
        this.fieldDeduplicator = new IndexFieldCapabilities.Deduplicator();
    }

    public FieldCapabilitiesIndexResponse fetch(final FieldCapabilitiesIndexRequest request) throws IOException {
        final ShardId shardId = request.shardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(request.shardId().getId());
        try (Engine.Searcher searcher = indexShard.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE)) {

            final SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
                shardId.id(),
                0,
                searcher,
                request::nowInMillis,
                null,
                request.runtimeFields()
            );

            if (canMatchShard(request, searchExecutionContext) == false) {
                return new FieldCapabilitiesIndexResponse(request.index(), Collections.emptyList(), false);
            }

            Set<String> fieldNames = new HashSet<>();
            for (String pattern : request.fields()) {
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
                        ft.meta()
                    );
                    responseMap.put(field, fieldDeduplicator.deduplicate(fieldCap));
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
                            ObjectMapper mapper = searchExecutionContext.getObjectMapper(parentField);
                            // Composite runtime fields do not have a mapped type for the root - check for null
                            if (mapper != null) {
                                String type = mapper.isNested() ? "nested" : "object";
                                IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(
                                    parentField,
                                    type,
                                    false,
                                    false,
                                    false,
                                    Collections.emptyMap()
                                );
                                responseMap.put(parentField, fieldDeduplicator.deduplicate(fieldCap));
                            }
                        }
                        dotIndex = parentField.lastIndexOf('.');
                    }
                }
            }
            return new FieldCapabilitiesIndexResponse(request.index(), responseMap.values(), true);
        }
    }

    private boolean canMatchShard(FieldCapabilitiesIndexRequest req, SearchExecutionContext searchExecutionContext) throws IOException {
        if (req.indexFilter() == null || req.indexFilter() instanceof MatchAllQueryBuilder) {
            return true;
        }
        assert req.nowInMillis() != 0L;
        ShardSearchRequest searchRequest = new ShardSearchRequest(req.shardId(), null, req.nowInMillis(), AliasFilter.EMPTY);
        searchRequest.source(new SearchSourceBuilder().query(req.indexFilter()));
        return SearchService.queryStillMatchesAfterRewrite(searchRequest, searchExecutionContext);
    }

}
