/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.migrate;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.script.Script;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

/**
 * Migrates documents from one index to a newly created index with a different mapping.
 */
public class TransportMigrateIndexAction extends TransportMasterNodeAction<MigrateIndexRequest, MigrateIndexResponse> {
    /**
     * Migrate documents from the source index to the destination index and refresh the destination index so the documents are visible.
     * There are two implementations of this in the Elasticsearch code base, a default one in Elasticsearch core that only migrates empty
     * indexes and a "real" one that uses {@code _reindex} to migrate non-empty indexes.
     */
    public interface DocumentMigrater {
        void migrateDocuments(String sourceIndex, String destinationIndex, Script script, TimeValue timeout, Client client,
                ActionListener<Void> listener);
    }
    /**
     * The running migrate actions on this node. All access to the map are synchronized on it.
     */
    private final Map<String, ActingOperation> runningOperations = new HashMap<>();
    /**
     * Shared instance of the client that doesn't assign any parent task information.
     */
    private final Client sharedClient;
    private final DocumentMigrater documentMigrater;

    @Inject
    public TransportMigrateIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
            ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Client client,
            DocumentMigrater documentMigrater) {
        super(settings, MigrateIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                MigrateIndexRequest::new);
        this.sharedClient = client;
        this.documentMigrater = documentMigrater;
    }

    @Override
    protected String executor() {
        // Since this task might block while it synchronizes to coalesce running tasks we shouldn't run this on the listener thread pool.
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected MigrateIndexResponse newResponse() {
        return new MigrateIndexResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(MigrateIndexRequest request, ClusterState state) {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(true, true, request.indicesOptions().expandWildcardsOpen(),
                request.indicesOptions().expandWildcardsClosed());
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request.indices()));
    }

    void withRunningOperation(String destinationIndex, Consumer<ActingOperation> consumer) {
        synchronized (runningOperations) {
            consumer.accept(runningOperations.get(destinationIndex));
        }
    }

    @Override
    protected final void masterOperation(MigrateIndexRequest request, ClusterState state, ActionListener<MigrateIndexResponse> listener)
            throws Exception {
        throw new UnsupportedOperationException("Task required");
    }

    @Override
    protected final void masterOperation(Task task, MigrateIndexRequest request, ClusterState state,
            ActionListener<MigrateIndexResponse> listener) {
        if (false == preflightChecks(request.getCreateIndexRequest(), state.metaData())) {
            // Hurray! No work to do!
            listener.onResponse(new MigrateIndexResponse(true, true));
            return;
        }

        Operation operation = buildOperation(task, request, state.metaData(), listener);
        ((MigrateIndexTask) task).setOperation(operation);
        operation.start();
    }

    /**
     * Run pre-flight checks to see if any migration needs to be performed.
     * @return true if a migration needs to be performed, false if the migration has already been completed
     */
    boolean preflightChecks(CreateIndexRequest createIndex, MetaData clusterMetaData) {
        AliasOrIndex index = clusterMetaData.getAliasAndIndexLookup().get(createIndex.index());
        if (index == null) {
            // Destination index doesn't exist, got to create it.
            return true;
        }            
        if (index.isAlias()) {
            throw new IllegalArgumentException("[" + createIndex.index() + "] doesn't exist but an alias of the same name does");
        }
        if (index.getIndices().size() != 1) {
            throw new IllegalStateException("Unexpected amount of metadata for index [" + index.getIndices() + "]");
        }
        IndexMetaData meta = index.getIndices().get(0);
        for (Alias expected : createIndex.aliases()) {
            AliasMetaData actual = meta.getAliases().get(expected.name());
            if (actual == null) {
                throw new IllegalArgumentException(
                        "[" + createIndex.index() + "] already exists but doesn't have the [" + expected.name() + "] alias");
            }
            try {
                if (expected.filter() == null) {
                    if (actual.filter() != null) {
                        String actualFilter = XContentHelper.convertToJson(new BytesArray(actual.filter().uncompressed()), false);
                        throw new IllegalArgumentException("[" + createIndex.index() + "] already exists and has the [" + expected.name()
                                + "] alias but the filter doesn't match. Expected [null] but got [" + actualFilter + "]");
                    }
                } else {
                    if (actual.filter() == null) {
                        throw new IllegalArgumentException("[" + createIndex.index() + "] already exists and has the [" + expected.name()
                            + "] alias but the filter doesn't match. Expected [" + expected.filter() + "] but got [null]");
                    }
                    /* filters have to match we would just map-ify and compare, but that isn't good enough because some xcontent types
                     * make floats and some make booleans.... so we have to convert both to json and *then* we can compare the maps....
                     */
                    String expectedJson = XContentHelper.convertToJson(new BytesArray(expected.filter()), false);
                    Map<String, Object> expectedFilterMap = XContentHelper.convertToMap(new BytesArray(expectedJson), false).v2();
                    String actualJson = XContentHelper.convertToJson(new BytesArray(actual.filter().uncompressed()), false);
                    Map<String, Object> actualFilterMap = XContentHelper.convertToMap(new BytesArray(actualJson), false).v2();
                    if (false == expectedFilterMap.equals(actualFilterMap)) {
                        throw new IllegalArgumentException("[" + createIndex.index() + "] already exists and has the [" + expected.name()
                                + "] alias but the filter doesn't match. Expected [" + expected.filter() + "] but got [" + actualJson
                                + "]");
                    }

                }
            } catch (IOException e) {
                throw new IllegalStateException("Error comparing filters for [" + createIndex.index() + "]", e);
            }
            if (false == Objects.equals(expected.indexRouting(), actual.indexRouting())) {
                throw new IllegalArgumentException("[" + createIndex.index() + "] already exists and has the [" + expected.name()
                        + "] alias but the index routing doesn't match. Expected [" + expected.indexRouting() + "] but got ["
                        + actual.indexRouting() + "]");
            }
            if (false == Objects.equals(expected.searchRouting(), actual.searchRouting())) {
                throw new IllegalArgumentException("[" + createIndex.index() + "] already exists and has the [" + expected.name()
                        + "] alias but the search routing doesn't match. Expected [" + expected.searchRouting() + "] but got ["
                        + actual.searchRouting() + "]");
            }
        }
        return false;
    }

    /**
     * Build the Operation for this request, either a {@link ActingOperation} is this is the first request on this index or an
     * {@link ObservingOperation} if there is already an {@linkplain ActingOperation} for this index.
     */
    Operation buildOperation(Task task, MigrateIndexRequest request, MetaData clusterMetaData,
            ActionListener<MigrateIndexResponse> listener) {
        synchronized (runningOperations) {
            ActingOperation currentlyRunning = runningOperations.get(request.getCreateIndexRequest().index());
            if (currentlyRunning != null) {
                // There is a request currently running for this index. We have to "follow" it.
                // NOCOMMIT make sure that the requests are the same....
                ObservingOperation newOperation = new ObservingOperation(currentlyRunning, listener);
                currentlyRunning.observers.add(newOperation);
                return newOperation;
            }
            // This Operation is the first concurrent attempt at this request.
            IndexMetaData sourceIndex = sourceIndexMetaData(clusterMetaData, request.getSourceIndex());
            ActingOperation newOperation = new ActingOperation(client(task), request, sourceIndex, listener);
            // Add our operation to the map so another migration for the same index can wait for this one to complete.
            runningOperations.put(request.getCreateIndexRequest().index(), newOperation);
            return newOperation;
        }
    }

    /**
     * Build the {@link Client} to use for this operation.
     */
    Client client(Task task) {
        TaskId thisTaskId = new TaskId(clusterService.localNode().getEphemeralId(), task.getId());
        return new ParentTaskAssigningClient(sharedClient, thisTaskId);
    }

    /**
     * Lookup information about the source index.
     */
    IndexMetaData sourceIndexMetaData(MetaData clusterMetaData, String name) {
        AliasOrIndex sourceAliasOrIndex = clusterMetaData.getAliasAndIndexLookup().get(name);
        if (sourceAliasOrIndex == null) {
            return null;
        }
        if (sourceAliasOrIndex.isAlias()) {
            throw new IllegalArgumentException("Can't migrate from an alias and [" + name + "] is an alias.");
        }
        return ((AliasOrIndex.Index)sourceAliasOrIndex).getIndex();
    }

    abstract class Operation {
        abstract void start();
    }

    class ObservingOperation extends Operation {
        /**
         * The operation that is actually running the request. We'll be notified when it completes.
         */
        final ActingOperation waitingFor;
        /**
         * The listener for responses of this operation. 
         */
        private final ActionListener<MigrateIndexResponse> listener;

        public ObservingOperation(ActingOperation waitingFor, ActionListener<MigrateIndexResponse> listener) {
            this.listener = listener;
            this.waitingFor = waitingFor;
        }

        @Override
        void start() {
            // Nothing to do. We just wait until waitingFor finishes and it'll call us.
        }
    }

    class ActingOperation extends Operation {
        /**
         * List of all observers waiting for this operation to complete. All accesses are synchronized on
         * {@link TransportMigrateIndexAction#runningOperations}.
         */
        final List<ObservingOperation> observers = new ArrayList<>();
        /**
         * The client to use when performing this operation.
         */
        private final Client client;
        /**
         * The request to perform this operation.
         */
        final MigrateIndexRequest request;
        /**
         * The source index. Null if there is no source.
         */
        @Nullable
        private final IndexMetaData sourceIndex;
        /**
         * The listener for responses of this operation. Fires the listeners of all {@link #observers}.
         */
        final ActionListener<MigrateIndexResponse> listener;

        /**
         * Build the operation, coalescing it into any currently running operation against the same index.
         */
        public ActingOperation(Client client, MigrateIndexRequest request, IndexMetaData sourceIndex,
                ActionListener<MigrateIndexResponse> listenerForThisRequest) {
            this.client = client;
            this.request = request;
            this.sourceIndex = sourceIndex;
            this.listener = new ActionListener<MigrateIndexResponse>() {
                @Override
                public void onResponse(MigrateIndexResponse response) {
                    sendResponse(listenerForThisRequest, l -> l.onResponse(response));
                }

                @Override
                public void onFailure(Exception e) {
                    sendResponse(listenerForThisRequest, l -> l.onFailure(e));
                }
            };
        }

        /**
         * Called on the {@linkplain ThreadPool.Names#GENERIC} thread pool if this is the first migration for this index.
         */
        @Override
        void start() {
            /* We can't just use the CreateIndexRequest the user provided because it contains aliases and we need to handle them specially.
             * So instead we create a copy and load it up. */
            CreateIndexRequest template = request.getCreateIndexRequest();
            CreateIndexRequest createIndex = new CreateIndexRequest(template.index(), template.settings());
            for (Map.Entry<String, String> mapping : template.mappings().entrySet()) {
                createIndex.mapping(mapping.getKey(), mapping.getValue());
            }
            createIndex.cause("migration");
            createIndex.timeout(request.timeout());
            createIndex.updateAllTypes(template.updateAllTypes());
            createIndex.waitForActiveShards(template.waitForActiveShards());
            // NOCOMMIT set the status
            client.admin().indices().create(createIndex, listener(response -> {
                if (response.isAcknowledged() == false) {
                    throw new ElasticsearchException("Timed out waiting to create [" + template.index() + "]");
                }
                if (response.isShardsAcked() == false) {
                    throw new ElasticsearchException("Timed out waiting for shards for [" + template.index() + "] to come online");
                }
                migrateDocuments();
            }));
        }

        /**
         * Called on the {@linkplain ThreadPool.Names#LISTENER} thread pool when the index has been successfully created.
         */
        void migrateDocuments() {
            if (sourceIndex == null) {
                // Source index doesn't exist, not documents to migrate.
                swapAliases();
            } else {
                documentMigrater.migrateDocuments(request.getSourceIndex(), request.getCreateIndexRequest().index(), request.getScript(),
                        request.timeout(), client, listener(v -> swapAliases()));
            }
        }

        /**
         * Called on the {@linkplain ThreadPool.Names#LISTENER} thread pool when the documents have been successfully migrated.
         */
        void swapAliases() {
            // NOCOMMIT remove the alias and the source index in the same atomic cluster state operation
            IndicesAliasesRequest aliases = new IndicesAliasesRequest();
            for (Alias alias: request.getCreateIndexRequest().aliases()) {
                AliasAction aliasAction = new AliasAction(AliasAction.Type.ADD, request.getCreateIndexRequest().index(),
                        alias.name());
                aliasAction.filter(alias.filter());
                aliasAction.searchRouting(alias.searchRouting());
                aliasAction.indexRouting(alias.indexRouting());
                aliases.addAliasAction(aliasAction);
            }
            // Strip all the aliases from the source indexes while we add aliases to the new index.
            if (sourceIndex != null) {
                for (ObjectCursor<String> alias : sourceIndex.getAliases().keys()) {
                    aliases.addAliasAction(new AliasAction(AliasAction.Type.REMOVE, sourceIndex.getIndex().getName(), alias.value));
                }
            }
            aliases.timeout(request.timeout());
            client.admin().indices().aliases(aliases, listener(response -> {
                if (false == response.isAcknowledged()) {
                    throw new ElasticsearchException("Timed out waiting to remove aliases");
                }
                if (sourceIndex == null) {
                    listener.onResponse(new MigrateIndexResponse(true, false));
                } else {
                    removeSourceIndex();
                }
            }));
        }

        /**
         * Called on the {@linkplain ThreadPool.Names#LISTENER} thread pool when the aliases have been successfully added to the new index.
         */
        void removeSourceIndex() {
            DeleteIndexRequest delete = new DeleteIndexRequest(request.getSourceIndex());
            delete.timeout(request.timeout());
            client.admin().indices().delete(delete, listener(response -> {
                if (false == response.isAcknowledged()) {
                    throw new ElasticsearchException("Timed out deleting [" + request.getSourceIndex() + "]");
                }
                listener.onResponse(new MigrateIndexResponse(true, false));
            }));
        }

        /**
         * Convert a consumer of successfully responses to and {@linkplain ActionListener} by delegating failures to the overall listener
         * for this action. This causes any failures to be sent back to the user as catastrophic failures.
         */
        private <T> ActionListener<T> listener(Consumer<T> onResponse) {
            return ActionListener.wrap(onResponse, listener::onFailure);
        }

        /**
         * Send a response to this operation's listener and the listeners for all waiting duplicates.
         */
        private void sendResponse(ActionListener<MigrateIndexResponse> listenerForThisRequest,
                Consumer<ActionListener<MigrateIndexResponse>> send) {
            class SendResponse extends AbstractRunnable {
                private final ActionListener<MigrateIndexResponse> listener;

                SendResponse(ActionListener<MigrateIndexResponse> listener) {
                    this.listener = listener;
                }

                @Override
                protected void doRun() throws Exception {
                    send.accept(listener);
                }

                public void onFailure(Exception e) {
                    // NOCOMMIT figure out what to do here. We've failed to send to a listener. Maybe just warn? 
                    throw new UnsupportedOperationException();
                }
            }
            synchronized (runningOperations) {
                try {
                    Executor executor = threadPool.executor(ThreadPool.Names.LISTENER);
                    executor.execute(new SendResponse(listenerForThisRequest));
                    for (ObservingOperation observer : observers) {
                        executor.execute(new SendResponse(observer.listener));
                    }
                } finally {
                    Operation removed = runningOperations.remove(request.getCreateIndexRequest().index());
                    assert removed == this;
                }
            }
        }
    }

    /**
     * {@linkplain DocumentMigrater} that never copies any documents, instead halting the process if there are any documents to copy.
     */
    public static class EmptyIndexDocumentMigrater implements DocumentMigrater {
        @Override
        public void migrateDocuments(String sourceIndex, String destinationIndex, Script script, TimeValue timeout, Client client,
                ActionListener<Void> listener) {
            SearchRequest search = new SearchRequest(sourceIndex).source(searchSource().size(0).terminateAfter(1));
            client.search(search, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    if (response.getFailedShards() > 0) {
                        onFailure(new ElasticsearchException("There were shard failures while checking if [" + sourceIndex
                                + "] is empty:  " + Arrays.toString(response.getShardFailures())));
                        return;
                    }
                    if (response.getHits().getTotalHits() > 0) {
                        onFailure(new UnsupportedOperationException("Without the reindex module Elasticsearch can only migrate from "
                                + "empty indexes and [" + sourceIndex + "] does not appear to be empty."));
                        return;
                    }
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
            
        }
    }
}
