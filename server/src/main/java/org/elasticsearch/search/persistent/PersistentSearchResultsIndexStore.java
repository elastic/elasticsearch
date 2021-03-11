/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

public class PersistentSearchResultsIndexStore {
    public static final String INDEX = ".persistent_search_results";
    public static final String ID_FIELD = "id";
    public static final String RESPONSE_FIELD = "response";
    public static final String EXPIRATION_TIME_FIELD = "expiration_time";
    public static final String REDUCED_SHARDS_INDEX_FIELD = "reduced_shards_index_field";

    private static final Setting<TimeValue> INITIAL_DELAY_SETTING = Setting.timeSetting("search.persistent.storage.initial_delay",
        TimeValue.timeValueMillis(50), TimeValue.timeValueMillis(50));
    private static final Setting<TimeValue> TIMEOUT_SETTING = Setting.timeSetting("search.persistent.storage.retry_timeout",
        TimeValue.timeValueSeconds(20), TimeValue.timeValueSeconds(1));

    static Settings settings() {
        return Settings.builder()
            .put("index.codec", "best_compression")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    private static XContentBuilder mappings() {
        try {
            return jsonBuilder()
                .startObject()
                    .startObject(SINGLE_MAPPING_NAME)
                        .startObject("_meta")
                            .field("version", Version.CURRENT)
                        .endObject()
                        .field("dynamic", "strict")
                        .startObject("properties")
                            .startObject(ID_FIELD)
                                .field("type", "keyword")
                            .endObject()
                            .startObject(RESPONSE_FIELD)
                                .field("type", "binary")
                            .endObject()
                            .startObject(REDUCED_SHARDS_INDEX_FIELD)
                                .field("type", "long")
                            .endObject()
                            .startObject(EXPIRATION_TIME_FIELD)
                                .field("type", "long")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + INDEX, e);
        }
    }

    public static List<SystemIndexDescriptor> getSystemIndexDescriptors() {
        return List.of(SystemIndexDescriptor.builder()
            .setIndexPattern(INDEX)
            .setDescription("persistent search results")
            .setPrimaryIndex(INDEX)
            .setMappings(mappings())
            .setSettings(settings())
            .setVersionMetaKey("version")
            .setOrigin("persistent_search")
            .build()
        );
    }

    private final Logger logger = LogManager.getLogger(PersistentSearchService.class);
    private final Client client;
    private final ThreadPool threadPool;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TimeValue initialRetryDelay;
    private final TimeValue timeout;

    public PersistentSearchResultsIndexStore(Client client,
                                             ThreadPool threadPool,
                                             NamedWriteableRegistry namedWriteableRegistry,
                                             Settings settings) {
        this.client = client;
        this.threadPool = threadPool;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.initialRetryDelay = INITIAL_DELAY_SETTING.get(settings);
        this.timeout = TIMEOUT_SETTING.get(settings);
    }

    public void storeShardResult(ShardSearchResult searchResult, ActionListener<String> storeListener) {
    }

    public void getShardResult(String id, ActionListener<ShardSearchResult> listener) {
    }

    public void storeResult(PersistentSearchResponse persistentSearchResponse, ActionListener<String> storeListener) {
        new StorageRetryListener<>(logger, threadPool, initialRetryDelay, timeout, storeListener) {
            @Override
            public void tryAction(ActionListener<String> listener) {
                try {
                    final IndexRequest indexRequest = new IndexRequest(INDEX)
                        .id(persistentSearchResponse.getId())
                        .versionType(VersionType.EXTERNAL)
                        .version(persistentSearchResponse.getVersion());

                    try (XContentBuilder builder = jsonBuilder()) {
                        indexRequest.source(persistentSearchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));
                    }

                    client.index(indexRequest, listener.delegateFailure((delegate, indexResponse) ->
                        delegate.onResponse(persistentSearchResponse.getId())));

                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }.run();
    }

    public void getPersistentSearchResponseAsync(String id, ActionListener<PersistentSearchResponse> getListener) {
        final GetRequest getRequest = new GetRequest(INDEX).id(id);
        new StorageRetryListener<>(logger, threadPool, initialRetryDelay, timeout, getListener) {
            @Override
            public void tryAction(ActionListener<PersistentSearchResponse> listener) {
                client.get(getRequest, listener.delegateFailure((delegate, getResponse) -> {
                    if (getResponse.isSourceEmpty()) {
                        delegate.onResponse(null);
                        return;
                    }

                    try {
                        final PersistentSearchResponse persistentSearchResponse =
                            PersistentSearchResponse.fromXContent(getResponse.getSource(),
                                getResponse.getVersion(),
                                namedWriteableRegistry
                            );
                        delegate.onResponse(persistentSearchResponse);
                    } catch (Exception e) {
                        delegate.onFailure(e);
                    }
                }));
            }
        }.run();
    }

    private abstract static class StorageRetryListener<R> extends RetryableAction<R> {
        private final Logger logger;
        private StorageRetryListener(Logger logger,
                                     ThreadPool threadPool,
                                     TimeValue initialDelay,
                                     TimeValue timeoutValue,
                                     ActionListener<R> listener) {
            super(logger, threadPool, initialDelay, timeoutValue, listener);
            this.logger = logger;
        }

        @Override
        public boolean shouldRetry(Exception e) {
            if (TransportActions.isShardNotAvailableException(e)
                || e instanceof ConnectTransportException
                || e instanceof ClusterBlockException) {
                return true;
            }
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof NodeClosedException || cause instanceof ConnectTransportException;
        }
    }

    public void deletePersistentSearchResults(List<String> persistentSearchResultIds, ActionListener<Collection<DeleteResponse>> listener) {
//        // TODO: delete based on TTL
        GroupedActionListener<DeleteResponse> groupedListener = new GroupedActionListener<>(listener, persistentSearchResultIds.size());
        for (String persistentSearchResultId : persistentSearchResultIds) {
            final DeleteRequest deleteRequest = client.prepareDelete(INDEX, persistentSearchResultId).request();
            client.delete(deleteRequest, groupedListener);
        }
    }
}
