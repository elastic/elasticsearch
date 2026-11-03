/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex;

import org.elasticsearch.Build;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.CONTEXT_ENGINE_ORIGIN;

/**
 * Owns the AI index registry system index and performs the registry reads and writes on behalf of the Context Engine
 * actions. Records are keyed by {@code <space>:<id>} so an id is unique within a space.
 */
public class AiIndexRegistryService {
    private static final Logger logger = LogManager.getLogger(AiIndexRegistryService.class);

    public static final String AI_INDEX_REGISTRY_ALIAS_NAME = ".context-engine-ai-indices";
    public static final String AI_INDEX_REGISTRY_INDEX_PREFIX = ".context-engine-ai-indices-";
    public static final String AI_INDEX_REGISTRY_INDEX_NAME_PATTERN = ".context-engine-ai-indices*";
    public static final int AI_INDEX_REGISTRY_INDEX_VERSION = 1;
    public static final String AI_INDEX_REGISTRY_CONCRETE_INDEX_NAME = AI_INDEX_REGISTRY_INDEX_PREFIX + AI_INDEX_REGISTRY_INDEX_VERSION;

    private final Client clientWithOrigin;

    public AiIndexRegistryService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, CONTEXT_ENGINE_ORIGIN);
    }

    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(AI_INDEX_REGISTRY_INDEX_NAME_PATTERN)
            .setPrimaryIndex(AI_INDEX_REGISTRY_CONCRETE_INDEX_NAME)
            .setDescription("Contains the Context Engine AI index registry")
            .setMappings(getIndexMappings())
            .setSettings(getIndexSettings())
            .setAliasName(AI_INDEX_REGISTRY_ALIAS_NAME)
            .setIndexFormat(AI_INDEX_REGISTRY_INDEX_VERSION)
            .setOrigin(CONTEXT_ENGINE_ORIGIN)
            .setThreadPools(ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    static String docId(String space, String id) {
        return space + ":" + id;
    }

    /**
     * Stores a record under {@code id} in {@code space}. With {@code create} the write fails with a version conflict when
     * the record exists; otherwise an existing record is replaced and keeps its creation date.
     */
    public void put(String id, AiIndex aiIndex, boolean create, ActionListener<Tuple<AiIndex, DocWriteResponse.Result>> listener) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        if (create) {
            index(aiIndex.withIdentityAndDates(id, aiIndex.space(), now, now), DocWriteRequest.OpType.CREATE, listener);
            return;
        }
        clientWithOrigin.prepareGet(AI_INDEX_REGISTRY_ALIAS_NAME, docId(aiIndex.space(), id)).execute(ActionListener.wrap(getResponse -> {
            Instant created = now;
            if (getResponse.isExists() && getResponse.isSourceEmpty() == false) {
                created = parse(getResponse.getSourceAsBytesRef().streamInput()).dateCreated();
            }
            index(aiIndex.withIdentityAndDates(id, aiIndex.space(), created, now), DocWriteRequest.OpType.INDEX, listener);
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                index(aiIndex.withIdentityAndDates(id, aiIndex.space(), now, now), DocWriteRequest.OpType.INDEX, listener);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private void index(AiIndex stored, DocWriteRequest.OpType opType, ActionListener<Tuple<AiIndex, DocWriteResponse.Result>> listener) {
        try {
            clientWithOrigin.prepareIndex(AI_INDEX_REGISTRY_ALIAS_NAME)
                .setId(docId(stored.space(), stored.id()))
                .setOpType(opType)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .setSource(stored.toXContent(jsonBuilder(), null))
                .execute(
                    listener.delegateFailureAndWrap((l, indexResponse) -> l.onResponse(Tuple.tuple(stored, indexResponse.getResult())))
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void get(String space, String id, ActionListener<AiIndex> listener) {
        clientWithOrigin.prepareGet(AI_INDEX_REGISTRY_ALIAS_NAME, docId(space, id)).execute(ActionListener.wrap(getResponse -> {
            if (getResponse.isExists() == false || getResponse.isSourceEmpty()) {
                listener.onFailure(notFound(space, id));
                return;
            }
            listener.onResponse(parse(getResponse.getSourceAsBytesRef().streamInput()));
        }, e -> listener.onFailure(e instanceof IndexNotFoundException ? notFound(space, id) : e)));
    }

    /**
     * Lists records sorted by id, optionally narrowed to one space and to records whose backing store is {@code dest}.
     */
    public void list(String space, String dest, int size, ActionListener<List<AiIndex>> listener) {
        try {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            if (space != null) {
                query.filter(QueryBuilders.termQuery(AiIndex.SPACE_FIELD.getPreferredName(), space));
            }
            if (dest != null) {
                query.filter(
                    QueryBuilders.termQuery(AiIndex.DEST_FIELD.getPreferredName() + "." + TypeValue.VALUE_FIELD.getPreferredName(), dest)
                );
            }
            SearchSourceBuilder source = new SearchSourceBuilder().query(query)
                .size(size)
                .sort(AiIndex.ID_FIELD.getPreferredName(), SortOrder.ASC);
            SearchRequest request = new SearchRequest(AI_INDEX_REGISTRY_ALIAS_NAME).source(source);
            clientWithOrigin.search(request, ActionListener.wrap(searchResponse -> {
                List<AiIndex> results = new ArrayList<>();
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    results.add(parse(hit.getSourceRef().streamInput()));
                }
                listener.onResponse(results);
            }, e -> {
                if (e instanceof IndexNotFoundException) {
                    listener.onResponse(List.of());
                } else {
                    listener.onFailure(e);
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void delete(String space, String id, ActionListener<Void> listener) {
        clientWithOrigin.prepareDelete(AI_INDEX_REGISTRY_ALIAS_NAME, docId(space, id))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
            .execute(ActionListener.wrap(deleteResponse -> {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    listener.onFailure(notFound(space, id));
                    return;
                }
                listener.onResponse(null);
            }, e -> listener.onFailure(e instanceof IndexNotFoundException ? notFound(space, id) : e)));
    }

    private static ResourceNotFoundException notFound(String space, String id) {
        return new ResourceNotFoundException("AI index [" + id + "] not found in space [" + space + "]");
    }

    private static AiIndex parse(java.io.InputStream source) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, source)) {
            return AiIndex.fromXContent(parser);
        }
    }

    private static XContentBuilder getIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("version", Build.current().version());
                builder.field(SystemIndexDescriptor.VERSION_META_KEY, AI_INDEX_REGISTRY_INDEX_VERSION);
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    keyword(builder, AiIndex.ID_FIELD.getPreferredName());
                    keyword(builder, AiIndex.SPACE_FIELD.getPreferredName());
                    builder.startObject(AiIndex.DESCRIPTION_FIELD.getPreferredName()).field("type", "text").endObject();
                    builder.startObject(AiIndex.MANAGED_FIELD.getPreferredName()).field("type", "boolean").endObject();
                    builder.startObject(AiIndex.MEMORY_ENABLED_FIELD.getPreferredName()).field("type", "boolean").endObject();
                    builder.startObject(AiIndex.DATE_CREATED_FIELD.getPreferredName()).field("type", "date").endObject();
                    builder.startObject(AiIndex.DATE_MODIFIED_FIELD.getPreferredName()).field("type", "date").endObject();
                    typeValue(builder, AiIndex.DEST_FIELD.getPreferredName());
                    typeValue(builder, AiIndex.AUTOMATIONS_FIELD.getPreferredName());
                    typeValue(builder, AiIndex.SOURCES_FIELD.getPreferredName());
                    typeValue(builder, AiIndex.TRACES_FIELD.getPreferredName());

                    builder.startObject(AiIndex.FEEDBACK_ANALYSIS_FIELD.getPreferredName());
                    builder.startObject("properties");
                    {
                        builder.startObject("enabled").field("type", "boolean").endObject();
                        keyword(builder, "agent_id");
                        builder.startObject("schedule");
                        builder.startObject("properties");
                        keyword(builder, "interval");
                        builder.endObject();
                        builder.endObject();
                        builder.startObject("signal_time_range");
                        builder.startObject("properties");
                        keyword(builder, "type");
                        keyword(builder, "from");
                        builder.endObject();
                        builder.endObject();
                        builder.startObject("signal_filter")
                            .field("type", "keyword")
                            .field("index", false)
                            .field("doc_values", false)
                            .endObject();
                        keyword(builder, "allowed_actions");
                    }
                    builder.endObject();
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            logger.fatal("Failed to build " + AI_INDEX_REGISTRY_CONCRETE_INDEX_NAME + " index mappings", e);
            throw new UncheckedIOException("Failed to build " + AI_INDEX_REGISTRY_CONCRETE_INDEX_NAME + " index mappings", e);
        }
    }

    private static void keyword(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name).field("type", "keyword").endObject();
    }

    private static void typeValue(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name);
        builder.startObject("properties");
        keyword(builder, TypeValue.TYPE_FIELD.getPreferredName());
        keyword(builder, TypeValue.VALUE_FIELD.getPreferredName());
        builder.endObject();
        builder.endObject();
    }

    private static Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, 100)
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), AI_INDEX_REGISTRY_INDEX_VERSION)
            .put("index.refresh_interval", "1s")
            .build();
    }
}
