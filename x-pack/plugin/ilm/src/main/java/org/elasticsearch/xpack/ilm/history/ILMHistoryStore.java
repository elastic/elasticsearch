/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.history;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.ilm.history.ILMHistoryTemplateRegistry.INDEX_TEMPLATE_VERSION;

/**
 * The {@link ILMHistoryStore} handles indexing {@link ILMHistoryItem} documents into the
 * appropriate index. It sets up a {@link BulkProcessor} for indexing in bulk, and handles creation
 * of the index/alias as needed for ILM policies.
 */
public class ILMHistoryStore implements Closeable {
    private static final Logger logger = LogManager.getLogger(ILMHistoryStore.class);

    public static final String ILM_HISTORY_INDEX_PREFIX = "ilm-history-" + INDEX_TEMPLATE_VERSION + "-";
    public static final String ILM_HISTORY_ALIAS = "ilm-history-" + INDEX_TEMPLATE_VERSION;

    private final boolean ilmHistoryEnabled;
    private final BulkProcessor processor;
    private final ThreadPool threadPool;

    public ILMHistoryStore(Settings nodeSettings, Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.ilmHistoryEnabled = LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.get(nodeSettings);
        this.threadPool = threadPool;

        this.processor = BulkProcessor.builder(
            new OriginSettingClient(client, INDEX_LIFECYCLE_ORIGIN)::bulk,
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    // Prior to actually performing the bulk, we should ensure the index exists, and
                    // if we were unable to create it or it was in a bad state, we should not
                    // attempt to index documents.
                    try {
                        final CompletableFuture<Boolean> indexCreated = new CompletableFuture<>();
                        ensureHistoryIndex(client, clusterService.state(), ActionListener.wrap(indexCreated::complete,
                            ex -> {
                                logger.warn("failed to create ILM history store index prior to issuing bulk request", ex);
                                indexCreated.completeExceptionally(ex);
                            }));
                        indexCreated.get(2, TimeUnit.MINUTES);
                    } catch (Exception e) {
                        logger.warn(new ParameterizedMessage("unable to index the following ILM history items:\n{}",
                            request.requests().stream()
                                .filter(dwr -> (dwr instanceof IndexRequest))
                                .map(dwr -> ((IndexRequest) dwr))
                                .map(IndexRequest::sourceAsMap)
                                .map(Object::toString)
                                .collect(Collectors.joining("\n"))), e);
                        throw new ElasticsearchException(e);
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    long items = request.numberOfActions();
                    if (logger.isTraceEnabled()) {
                        logger.trace("indexed [{}] items into ILM history index [{}]", items,
                            Arrays.stream(response.getItems())
                                .map(BulkItemResponse::getIndex)
                                .distinct()
                                .collect(Collectors.joining(",")));
                    }
                    if (response.hasFailures()) {
                        Map<String, String> failures = Arrays.stream(response.getItems())
                            .filter(BulkItemResponse::isFailed)
                            .collect(Collectors.toMap(BulkItemResponse::getId, BulkItemResponse::getFailureMessage));
                        logger.error("failures: [{}]", failures);
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    long items = request.numberOfActions();
                    logger.error(new ParameterizedMessage("failed to index {} items into ILM history index", items), failure);
                }
            })
            .setBulkActions(100)
            .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .setConcurrentRequests(1)
            .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(1000), 3))
            .build();
    }

    /**
     * Attempts to asynchronously index an ILM history entry
     */
    public void putAsync(ILMHistoryItem item) {
        if (ilmHistoryEnabled == false) {
            logger.trace("not recording ILM history item because [{}] is [false]: [{}]",
                LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.getKey(), item);
            return;
        }
        logger.trace("queueing ILM history item for indexing [{}]: [{}]", ILM_HISTORY_ALIAS, item);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            item.toXContent(builder, ToXContent.EMPTY_PARAMS);
            IndexRequest request = new IndexRequest(ILM_HISTORY_ALIAS).source(builder);
            // TODO: remove the threadpool wrapping when the .add call is non-blocking
            //  (it can currently execute the bulk request occasionally)
            //  see: https://github.com/elastic/elasticsearch/issues/50440
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    processor.add(request);
                } catch (Exception e) {
                    logger.error(new ParameterizedMessage("failed add ILM history item to queue for index [{}]: [{}]",
                        ILM_HISTORY_ALIAS, item), e);
                }
            });
        } catch (IOException exception) {
            logger.error(new ParameterizedMessage("failed to queue ILM history item in index [{}]: [{}]",
                ILM_HISTORY_ALIAS, item), exception);
        }
    }

    /**
     * Checks if the ILM history index exists, and if not, creates it.
     *
     * @param client  The client to use to create the index if needed
     * @param state   The current cluster state, to determine if the alias exists
     * @param listener Called after the index has been created. `onResponse` called with `true` if the index was created,
     *                `false` if it already existed.
     */
    static void ensureHistoryIndex(Client client, ClusterState state, ActionListener<Boolean> listener) {
        final String initialHistoryIndexName = ILM_HISTORY_INDEX_PREFIX + "000001";
        final AliasOrIndex ilmHistory = state.metaData().getAliasAndIndexLookup().get(ILM_HISTORY_ALIAS);
        final AliasOrIndex initialHistoryIndex = state.metaData().getAliasAndIndexLookup().get(initialHistoryIndexName);

        if (ilmHistory == null && initialHistoryIndex == null) {
            // No alias or index exists with the expected names, so create the index with appropriate alias
            logger.debug("creating ILM history index [{}]", initialHistoryIndexName);
            client.admin().indices().prepareCreate(initialHistoryIndexName)
                .setWaitForActiveShards(1)
                .addAlias(new Alias(ILM_HISTORY_ALIAS)
                    .writeIndex(true))
                .execute(new ActionListener<CreateIndexResponse>() {
                    @Override
                    public void onResponse(CreateIndexResponse response) {
                        listener.onResponse(true);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof ResourceAlreadyExistsException) {
                            // The index didn't exist before we made the call, there was probably a race - just ignore this
                            logger.debug("index [{}] was created after checking for its existence, likely due to a concurrent call",
                                initialHistoryIndexName);
                            listener.onResponse(false);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                });
        } else if (ilmHistory == null) {
            // alias does not exist but initial index does, something is broken
            listener.onFailure(new IllegalStateException("ILM history index [" + initialHistoryIndexName +
                "] already exists but does not have alias [" + ILM_HISTORY_ALIAS + "]"));
        } else if (ilmHistory.isAlias() && ilmHistory instanceof AliasOrIndex.Alias) {
            if (((AliasOrIndex.Alias) ilmHistory).getWriteIndex() != null) {
                // The alias exists and has a write index, so we're good
                listener.onResponse(false);
            } else {
                // The alias does not have a write index, so we can't index into it
                listener.onFailure(new IllegalStateException("ILM history alias [" + ILM_HISTORY_ALIAS + "does not have a write index"));
            }
        } else if (ilmHistory.isAlias() == false) {
            // This is not an alias, error out
            listener.onFailure(new IllegalStateException("ILM history alias [" + ILM_HISTORY_ALIAS +
                "] already exists as concrete index"));
        } else {
            logger.error("unexpected IndexOrAlias for [{}]: [{}]", ILM_HISTORY_ALIAS, ilmHistory);
            assert false : ILM_HISTORY_ALIAS + " cannot be both an alias and not an alias simultaneously";
        }
    }

    @Override
    public void close() {
        try {
            processor.awaitClose(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("failed to shut down ILM history bulk processor after 10 seconds", e);
        }
    }
}
