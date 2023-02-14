/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * A service that manages the persistent {@link Engine} configurations.
 */
public class EngineIndexService {
    private static final Logger logger = LogManager.getLogger(EngineIndexService.class);
    public static final String ENGINE_ALIAS_NAME = ".engine";
    public static final String ENGINE_CONCRETE_INDEX_NAME = ".engine-1";
    public static final String ENGINE_INDEX_NAME_PATTERN = ".engine-*";
    public static final String ENGINE_ORIGIN = "engine";

    private final Client clientWithOrigin;
    private final BigArrays bigArrays;

    public EngineIndexService(Client client, BigArrays bigArrays) {
        this.clientWithOrigin = new OriginSettingClient(client, ENGINE_ORIGIN);
        this.bigArrays = bigArrays;
    }

    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(ENGINE_INDEX_NAME_PATTERN)
            .setPrimaryIndex(ENGINE_CONCRETE_INDEX_NAME)
            .setDescription("Contains auth token data")
            .setMappings(getIndexMappings())
            .setSettings(getIndexSettings())
            .setAliasName(ENGINE_ALIAS_NAME)
            .setVersionMetaKey("version")
            .setOrigin(ENGINE_ORIGIN)
            .setThreadPools(ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    private static Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, 100)
            .put("index.refresh_interval", "1s")
            .build();
    }

    private static XContentBuilder getIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("version", Version.CURRENT.toString());
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject("name");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("indices");
                    builder.field("type", "keyword");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            logger.fatal("Failed to build " + ENGINE_CONCRETE_INDEX_NAME + " index mappings", e);
            throw new UncheckedIOException("Failed to build " + ENGINE_CONCRETE_INDEX_NAME + " index mappings", e);
        }
    }

    /**
     * Gets the {@link Engine} from the index if present, or delegate a {@link ResourceNotFoundException} failure to the provided
     * listener if not.
     *
     * @param resourceName The resource name.
     * @param respListener The action listener to invoke on response/failure.
     */
    public void getEngine(String resourceName, ActionListener<Engine> respListener) {
        final GetRequest getRequest = new GetRequest(ENGINE_CONCRETE_INDEX_NAME).id(resourceName).realtime(true);
        clientWithOrigin.get(getRequest, respListener.delegateFailure((listener, getResponse) -> {
            if (getResponse.isExists() == false) {
                listener.onFailure(new ResourceNotFoundException(resourceName));
                return;
            }
            final Engine res;
            try {
                final BytesReference source = getResponse.getSourceInternal();
                res = Engine.fromXContentBytes(getResponse.getId(), source, XContentType.JSON);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(res);
        }));
    }

    /**
     * Creates or updates the {@link Engine} in the underlying index.
     *
     * @param engine The engine object.
     * @param respListener The action listener to invoke on response/failure.
     */
    public void putEngine(Engine engine, ActionListener<IndexResponse> respListener) {
        try {
            final ReleasableBytesStreamOutput buffer = new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking());
            final XContentBuilder source = XContentFactory.jsonBuilder(buffer);
            respListener = ActionListener.runBefore(respListener, buffer::close);
            engine.toXContent(source, EMPTY_PARAMS);
            // do not close the buffer or the XContentBuilder until the IndexRequest is completed (i.e., listener is notified);
            // otherwise, we underestimate the memory usage in case the circuit breaker does not use the real memory usage.
            source.flush();
            final IndexRequest indexRequest = new IndexRequest(ENGINE_ALIAS_NAME).opType(DocWriteRequest.OpType.INDEX)
                .id(engine.name())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(buffer.bytes(), source.contentType());
            clientWithOrigin.index(indexRequest, respListener);
        } catch (Exception e) {
            respListener.onFailure(e);
        }
    }

    /**
     * Deletes the provided {@param engineName} in the underlying index, or delegate a failure to the provided
     * listener if the resource does not exist or failed to delete.
     *
     * @param engineName The name of the {@link Engine} to delete.
     * @param respListener The action listener to invoke on response/failure.
     *
     */
    public void deleteEngine(String engineName, ActionListener<DeleteResponse> respListener) {
        try {
            final DeleteRequest deleteRequest = new DeleteRequest(ENGINE_ALIAS_NAME).id(engineName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            clientWithOrigin.delete(deleteRequest, respListener);
        } catch (Exception e) {
            respListener.onFailure(e);
        }
    }

    /**
     * List the {@link Engine} in ascending order of their names.
     *
     * @param from From index to start the search from.
     * @param size The maximum number of {@link Engine} to return.
     * @param respListener The action listener to invoke on response/failure.
     */
    public void listEngine(int from, int size, ActionListener<SearchResponse> respListener) {
        try {
            final SearchSourceBuilder source = new SearchSourceBuilder().from(from)
                .size(size)
                .docValueField("name")
                .docValueField("indices")
                .storedFields(Collections.singletonList("_none_"))
                .sort("name", SortOrder.ASC);
            final SearchRequest req = new SearchRequest(ENGINE_ALIAS_NAME).source(source);
            clientWithOrigin.search(req, respListener);
        } catch (Exception e) {
            respListener.onFailure(e);
        }
    }
}
