/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.singletonMap;

/**
 * Transport action used to retrieve the mappings related to fields that belong to a specific index
 */
public class TransportGetFieldMappingsIndexAction extends TransportSingleShardAction<
    GetFieldMappingsIndexRequest,
    GetFieldMappingsResponse> {

    private static final String ACTION_NAME = GetFieldMappingsAction.NAME + "[index]";
    public static final ActionType<GetFieldMappingsResponse> TYPE = new ActionType<>(ACTION_NAME);

    private final IndicesService indicesService;

    @Inject
    public TransportGetFieldMappingsIndexAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            GetFieldMappingsIndexRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indicesService;
    }

    @Override
    protected boolean resolveIndex(GetFieldMappingsIndexRequest request) {
        // internal action, index already resolved
        return false;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        // Will balance requests between shards
        return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
    }

    @Override
    protected GetFieldMappingsResponse shardOperation(final GetFieldMappingsIndexRequest request, ShardId shardId) {
        assert shardId != null;
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        Predicate<String> metadataFieldPredicate = (f) -> indexService.mapperService().isMetadataField(f);
        Predicate<String> fieldPredicate = metadataFieldPredicate.or(indicesService.getFieldFilter().apply(shardId.getIndexName()));

        MappingLookup mappingLookup = indexService.mapperService().mappingLookup();
        Map<String, FieldMappingMetadata> fieldMapping = findFieldMappings(fieldPredicate, mappingLookup, request);
        return new GetFieldMappingsResponse(singletonMap(shardId.getIndexName(), fieldMapping));
    }

    @Override
    protected Writeable.Reader<GetFieldMappingsResponse> getResponseReader() {
        return GetFieldMappingsResponse::new;
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_READ, request.concreteIndex());
    }

    private static final ToXContent.Params includeDefaultsParams = new ToXContent.Params() {

        static final String INCLUDE_DEFAULTS = "include_defaults";

        @Override
        public String param(String key) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return defaultValue;
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }

        @Override
        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }
    };

    private static Map<String, FieldMappingMetadata> findFieldMappings(
        Predicate<String> fieldPredicate,
        MappingLookup mappingLookup,
        GetFieldMappingsIndexRequest request
    ) {
        // TODO the logic here needs to be reworked to also include runtime fields. Though matching is against mappers rather
        // than field types, and runtime fields are mixed with ordinary fields in FieldTypeLookup
        Map<String, FieldMappingMetadata> fieldMappings = new HashMap<>();
        for (String field : request.fields()) {
            if (Regex.isMatchAllPattern(field)) {
                for (Mapper fieldMapper : mappingLookup.fieldMappers()) {
                    addFieldMapper(fieldPredicate, fieldMapper.fullPath(), fieldMapper, fieldMappings, request.includeDefaults());
                }
            } else if (Regex.isSimpleMatchPattern(field)) {
                for (Mapper fieldMapper : mappingLookup.fieldMappers()) {
                    if (Regex.simpleMatch(field, fieldMapper.fullPath())) {
                        addFieldMapper(fieldPredicate, fieldMapper.fullPath(), fieldMapper, fieldMappings, request.includeDefaults());
                    }
                }
            } else {
                // not a pattern
                Mapper fieldMapper = mappingLookup.getMapper(field);
                if (fieldMapper != null) {
                    addFieldMapper(fieldPredicate, field, fieldMapper, fieldMappings, request.includeDefaults());
                }
            }
        }
        return Collections.unmodifiableMap(fieldMappings);
    }

    private static void addFieldMapper(
        Predicate<String> fieldPredicate,
        String field,
        Mapper fieldMapper,
        Map<String, FieldMappingMetadata> fieldMappings,
        boolean includeDefaults
    ) {
        if (fieldMappings.containsKey(field)) {
            return;
        }
        if (fieldPredicate.test(field)) {
            try {
                BytesReference bytes = XContentHelper.toXContent(
                    fieldMapper,
                    XContentType.JSON,
                    includeDefaults ? includeDefaultsParams : ToXContent.EMPTY_PARAMS,
                    false
                );
                fieldMappings.put(field, new FieldMappingMetadata(fieldMapper.fullPath(), bytes));
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize XContent of field [" + field + "]", e);
            }
        }
    }
}
