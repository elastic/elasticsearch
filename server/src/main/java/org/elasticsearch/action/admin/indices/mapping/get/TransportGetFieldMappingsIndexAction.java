/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.MappingBuilder;
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
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
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
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            projectResolver,
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
    protected ShardsIterator shards(ProjectState state, InternalRequest request) {
        // Will balance requests between shards
        return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
    }

    @Override
    protected GetFieldMappingsResponse shardOperation(final GetFieldMappingsIndexRequest request, ShardId shardId) {
        assert shardId != null;
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        MapperService mapperService = indexService.mapperService();
        Predicate<String> metadataFieldPredicate = mapperService::isMetadataField;
        Predicate<String> fieldPredicate = metadataFieldPredicate.or(indicesService.getFieldFilter().apply(shardId.getIndexName()));

        DocumentMapper documentMapper = mapperService.documentMapper();
        if (documentMapper == null) {
            return new GetFieldMappingsResponse(Map.of(shardId.getIndexName(), Map.of()));
        }
        MappingBuilder mappingBuilder;
        // We re-parse mappings here which may emit deprecation warnings. We don't want these to appear
        // in a get-mappings request, so we use a new ThreadContext to swallow them and avoid posting
        // them back to the client.
        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            mappingBuilder = mapperService.parseMappings(documentMapper.mappingSource(), MergeReason.MAPPING_RECOVERY);
        }
        Map<String, FieldMappingMetadata> fieldMapping = findFieldMappings(fieldPredicate, mappingBuilder, request);
        assert sameExceptGeneratedFields(
            fieldMapping,
            findFieldMappingsFromMappers(fieldPredicate, documentMapper.mappers(), request),
            generatedFieldPaths(documentMapper.mappers())
        ) : "field mappings written from builders differ from those written from mappers";
        return new GetFieldMappingsResponse(singletonMap(shardId.getIndexName(), fieldMapping));
    }

    @Override
    protected Writeable.Reader<GetFieldMappingsResponse> getResponseReader() {
        return GetFieldMappingsResponse::new;
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ProjectState state, InternalRequest request) {
        return state.blocks().indexBlockedException(state.projectId(), ClusterBlockLevel.METADATA_READ, request.concreteIndex());
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

    static Map<String, FieldMappingMetadata> findFieldMappings(
        Predicate<String> fieldPredicate,
        MappingBuilder mappingBuilder,
        GetFieldMappingsIndexRequest request
    ) {
        // TODO the logic here needs to be reworked to also include runtime fields. Though matching is against mappers rather
        // than field types, and runtime fields are mixed with ordinary fields in FieldTypeLookup
        Map<String, FieldMappingMetadata> fieldMappings = new HashMap<>();
        // simpleMatch is a glob for a pattern, everything for "*", and equality for a field name: the three ways of asking
        mappingBuilder.collectFieldBuilders(MergeReason.MAPPING_RECOVERY, (path, builder) -> {
            if (Regex.simpleMatch(request.fields(), path)) {
                addField(fieldPredicate, path, builder, fieldMappings, request.includeDefaults());
            }
        });
        return Collections.unmodifiableMap(fieldMappings);
    }

    /**
     * Paths of all mappers that are internally generated (eg TextFieldMapper's special sub-mappers
     * for indexed prefixes or phrases).  These should not be reported, as they are not part of the
     * user-defined mappings.
     */
    private static Set<String> generatedFieldPaths(MappingLookup mappingLookup) {
        Set<String> generated = new HashSet<>();
        for (Mapper mapper : mappingLookup.fieldMappers()) {
            if (mapper instanceof FieldMapper fieldMapper) {
                Set<Mapper> multiFields = Collections.newSetFromMap(new IdentityHashMap<>());
                fieldMapper.multiFields().forEach(multiFields::add);
                for (Mapper child : fieldMapper) {
                    if (multiFields.contains(child) == false) {
                        addPathsOfSubtree(child, generated);
                    }
                }
            }
        }
        return generated;
    }

    private static void addPathsOfSubtree(Mapper mapper, Set<String> paths) {
        paths.add(mapper.fullPath());
        for (Mapper child : mapper) {
            addPathsOfSubtree(child, paths);
        }
    }

    /**
     * Check equivalence between mappings generated from a Builder and from a serialized Mapper.  Mappers
     * also report internally generated Mappers, which we want to avoid because these do not appear in the
     * actual mappings json; therefore this check ignores these generated sub-mappings.
     */
    private static boolean sameExceptGeneratedFields(
        Map<String, FieldMappingMetadata> fromBuilders,
        Map<String, FieldMappingMetadata> fromMappers,
        Set<String> generated
    ) {
        for (Map.Entry<String, FieldMappingMetadata> entry : fromMappers.entrySet()) {
            FieldMappingMetadata fromBuilder = fromBuilders.get(entry.getKey());
            boolean same = fromBuilder == null ? generated.contains(entry.getKey()) : fromBuilder.equals(entry.getValue());
            if (same == false) {
                return false;
            }
        }
        return fromMappers.keySet().containsAll(fromBuilders.keySet());
    }

    /**
     * Used to assert equality with the FieldMappings built from the mappings source
     */
    static Map<String, FieldMappingMetadata> findFieldMappingsFromMappers(
        Predicate<String> fieldPredicate,
        MappingLookup mappingLookup,
        GetFieldMappingsIndexRequest request
    ) {
        Map<String, FieldMappingMetadata> fieldMappings = new HashMap<>();
        for (String field : request.fields()) {
            if (Regex.isMatchAllPattern(field)) {
                for (Mapper fieldMapper : mappingLookup.fieldMappers()) {
                    addField(fieldPredicate, fieldMapper.fullPath(), fieldMapper, fieldMappings, request.includeDefaults());
                }
            } else if (Regex.isSimpleMatchPattern(field)) {
                for (Mapper fieldMapper : mappingLookup.fieldMappers()) {
                    if (Regex.simpleMatch(field, fieldMapper.fullPath())) {
                        addField(fieldPredicate, fieldMapper.fullPath(), fieldMapper, fieldMappings, request.includeDefaults());
                    }
                }
            } else {
                // not a pattern
                Mapper fieldMapper = mappingLookup.getMapper(field);
                if (fieldMapper != null) {
                    addField(fieldPredicate, field, fieldMapper, fieldMappings, request.includeDefaults());
                }
            }
        }
        return Collections.unmodifiableMap(fieldMappings);
    }

    private static void addField(
        Predicate<String> fieldPredicate,
        String fullPath,
        ToXContent field,
        Map<String, FieldMappingMetadata> fieldMappings,
        boolean includeDefaults
    ) {
        if (fieldMappings.containsKey(fullPath)) {
            return;
        }
        if (fieldPredicate.test(fullPath)) {
            try {
                BytesReference bytes = XContentHelper.toXContent(
                    field,
                    XContentType.JSON,
                    includeDefaults ? includeDefaultsParams : ToXContent.EMPTY_PARAMS,
                    false
                );
                fieldMappings.put(fullPath, new FieldMappingMetadata(fullPath, bytes));
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize XContent of field [" + fullPath + "]", e);
            }
        }
    }
}
