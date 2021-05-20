/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.singletonMap;

/**
 * Transport action used to retrieve the mappings related to fields that belong to a specific index
 */
public class TransportGetFieldMappingsIndexAction
        extends TransportSingleShardAction<GetFieldMappingsIndexRequest, GetFieldMappingsResponse> {

    private static final String ACTION_NAME = GetFieldMappingsAction.NAME + "[index]";
    public static final ActionType<GetFieldMappingsResponse> TYPE = new ActionType<>(ACTION_NAME, GetFieldMappingsResponse::new);

    protected final ClusterService clusterService;
    private final IndicesService indicesService;

    @Inject
    public TransportGetFieldMappingsIndexAction(ClusterService clusterService, TransportService transportService,
                                                IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters,
                                                IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                GetFieldMappingsIndexRequest::new, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected boolean resolveIndex(GetFieldMappingsIndexRequest request) {
        //internal action, index already resolved
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
        Predicate<String> baseFieldPredicate = metadataFieldPredicate.or(indicesService.getFieldFilter().apply(shardId.getIndexName()));

        DocumentMapper documentParser = indexService.mapperService().documentMapper();
        if (documentParser == null) {
            // no mappings
            return new GetFieldMappingsResponse(singletonMap(shardId.getIndexName(), Collections.emptyMap()));
        }

        Predicate<String> fieldPredicate = fieldPredicate(baseFieldPredicate, request.fields());
        ToXContent.Params params = request.includeDefaults()
            ? new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"))
            : ToXContent.EMPTY_PARAMS;

        Map<String, FieldMappingMetadata> mappings = new HashMap<>();

        documentParser.mapping().forEachMapper(m -> {
            FieldMappingMetadata metadata = buildFieldMappingMetadata(fieldPredicate, m.name(), m, params);
            if (metadata != null) {
                mappings.put(metadata.fullName(), metadata);
            }
        });

        for (RuntimeField rf : documentParser.mapping().runtimeFields()) {
            FieldMappingMetadata metadata = buildFieldMappingMetadata(fieldPredicate, rf.name(), rf, params);
            if (metadata != null) {
                mappings.put(metadata.fullName(), metadata);
            }
        }

        return new GetFieldMappingsResponse(singletonMap(shardId.getIndexName(), mappings));
    }

    @Override
    protected Writeable.Reader<GetFieldMappingsResponse> getResponseReader() {
        return GetFieldMappingsResponse::new;
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_READ, request.concreteIndex());
    }

    private static Predicate<String> fieldPredicate(Predicate<String> baseFieldPredicate, String[] fields) {
        Automaton fieldAutomaton = Regex.simpleMatchToAutomaton(fields);
        CharacterRunAutomaton fieldMatcher = new CharacterRunAutomaton(fieldAutomaton);
        return baseFieldPredicate.or(fieldMatcher::run);
    }

    private static FieldMappingMetadata buildFieldMappingMetadata(
        Predicate<String> fieldPredicate,
        String field,
        ToXContent mapping,
        ToXContent.Params params
    ) {
        if (fieldPredicate.test(field) == false) {
            return null;
        }
        try {
            BytesReference bytes = XContentHelper.toXContent(mapping, XContentType.JSON, params, false);
            return new FieldMappingMetadata(field, bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
