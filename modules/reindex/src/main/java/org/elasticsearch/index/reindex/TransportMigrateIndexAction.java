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

package org.elasticsearch.index.reindex;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexAction;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexRequest;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexResponse;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexTask;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class TransportMigrateIndexAction extends TransportMasterNodeAction<MigrateIndexRequest, MigrateIndexResponse> {
    /**
     * The running migrate actions on this node. All access to the map are synchronized on it.
     */
    private final Map<String, MigrateIndexTask> runningTasks = new HashMap<>();
    private final Client client;

    @Inject
    public TransportMigrateIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
            ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Client client) {
        super(settings, MigrateIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                MigrateIndexRequest::new);
        this.client = client; // NOCOMMIT ParentTaskAssigningClient
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

    /**
     * Get the running task to migrate to {@code destinationIndex} if there is one, null if there isn't one.
     */
    MigrateIndexTask getRunningTask(String destinationIndex) {
        synchronized (runningTasks) {
            return runningTasks.get(destinationIndex);
        }
    }

    @Override
    protected final void masterOperation(MigrateIndexRequest request, ClusterState state, ActionListener<MigrateIndexResponse> listener)
            throws Exception {
        throw new UnsupportedOperationException("Task required");
    }

    @Override
    protected final void masterOperation(Task t, MigrateIndexRequest request, ClusterState state,
            ActionListener<MigrateIndexResponse> listener) {
        MigrateIndexTask task = (MigrateIndexTask) t;
        task.setListener(listener);
        if (preflightChecks(task.getRequest().getCreateIndexRequest(), state.metaData())) {
            coalesceConcurrentRequestsToCreateSameIndex(task);
        } else {
            finishHim(task, new MigrateIndexResponse(true, true));
        }
    }

    /**
     * Run pre-flight checks to see if any migration needs to be performed.
     * @return true if a migration needs to be performed, false if the migration has already been completed
     */
    boolean preflightChecks(CreateIndexRequest createIndex, MetaData clusterMetaData) {
        AliasOrIndex index = clusterMetaData.getAliasAndIndexLookup().get(createIndex.index());
        if (index == null) {
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

    void coalesceConcurrentRequestsToCreateSameIndex(MigrateIndexTask task) {
        synchronized (runningTasks) {
            MigrateIndexTask currentlyRunning = runningTasks.get(task.getRequest().getCreateIndexRequest().index());
            if (currentlyRunning != null) {
                currentlyRunning.addDuplicate(task);
                /* 
                 * Return early here because we've done all we need to do to handle this request - we'll get the response when
                 * currentlyRunning finishes.
                 */
                return;
            }
            // Add our task to the map so another migration for the same index can wait for this one to complete.
            runningTasks.put(task.getRequest().getCreateIndexRequest().index(), task);
            // Wrap the listener in one that'll fire all 
            ActionListener<MigrateIndexResponse> originalListener = task.getListener();
            task.setListener(new ActionListener<MigrateIndexResponse>() {
                @Override
                public void onResponse(MigrateIndexResponse response) {
                    sendResponse(task, originalListener, l -> l.onResponse(response));
                }

                @Override
                public void onFailure(Exception e) {
                    sendResponse(task, originalListener, l -> l.onFailure(e));
                }
            });
        }
        // Start the actual migration outside of the synchronized block - we don't need the lock again until we send the response.
        startMigration(task);
    }

    void sendResponse(MigrateIndexTask task, ActionListener<MigrateIndexResponse> originalListener,
            Consumer<ActionListener<MigrateIndexResponse>> send) {
        synchronized (runningTasks) {
            try {
                Executor executor = threadPool.executor(ThreadPool.Names.LISTENER); 
                executor.execute(() -> send.accept(originalListener));
                for (MigrateIndexTask duplicate : task.getDuplicates()) {
                    executor.execute(new AbstractRunnable() {
                        @Override
                        protected void doRun() throws Exception {
                            send.accept(duplicate.getListener());
                        }

                        public void onFailure(Exception e) {
                            duplicate.getListener().onFailure(e);
                        }
                    });
                }
            } finally {
                MigrateIndexTask removed = runningTasks.remove(task.getRequest().getCreateIndexRequest().index());
                assert removed == task;
            }
        }
    }

    void startMigration(MigrateIndexTask task) {
        /* We can't just use the CreateIndexRequest the user provided because it contains aliases and we need to handle them specially. So
         * instead we create a copy and load it up. */
        CreateIndexRequest template = task.getRequest().getCreateIndexRequest();
        CreateIndexRequest createIndex = new CreateIndexRequest(template.index(), template.settings());
        for (Map.Entry<String, String> mapping : template.mappings().entrySet()) {
            createIndex.mapping(mapping.getKey(), mapping.getValue());
        }
        createIndex.cause("migration");
        createIndex.timeout(task.getRequest().timeout());
        createIndex.updateAllTypes(template.updateAllTypes());
        createIndex.waitForActiveShards(template.waitForActiveShards());
        // NOCOMMIT set the status
        client.admin().indices().create(createIndex, new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse response) {
                if (response.isAcknowledged() == false) {
                    throw new ElasticsearchException("Timed out waiting to create [" + template.index() + "]");
                }
                if (response.isShardsAcked() == false) {
                    // NOCOMMIT make double sure these exceptions are passed back
                    throw new ElasticsearchException("Timed out waiting for shards for [" + template.index() + "] to come online");
                }
                createdIndex(task);
            }

            @Override
            public void onFailure(Exception e) {
                task.getListener().onFailure(e);
            }
        });
    }

    /**
     * Called on the {@linkPlain ThreadPool.Names.LISTENER} thread pool when the index has been successfully created.
     */
    void createdIndex(MigrateIndexTask task) {
        ReindexRequest reindex = new ReindexRequest(
                new SearchRequest(task.getRequest().getSourceIndex()),
                new IndexRequest(task.getRequest().getCreateIndexRequest().index()));
        reindex.setScript(task.getRequest().getScript());
        reindex.setTimeout(task.getRequest().timeout());
        // Refresh so the documents are visible when we switch the aliases
        reindex.setRefresh(true); 
        // NOCOMMIT need to link up the status so the migration action's status looks like reindex's status while this is happening
        client.execute(ReindexAction.INSTANCE, reindex, new ActionListener<BulkIndexByScrollResponse>() {
            @Override
            public void onResponse(BulkIndexByScrollResponse response) {
                if (false == response.getSearchFailures().isEmpty() || false == response.getBulkFailures().isEmpty()) {
                    throw new ElasticsearchException("Reindex failed " + response.getSearchFailures() + " " + response.getBulkFailures());
                }
                migratedDocuments(task);
            }

            @Override
            public void onFailure(Exception e) {
                task.getListener().onFailure(e);
            }
        });
    }

    void migratedDocuments(MigrateIndexTask task) {
        // TODO we could certainly do better here, removing the alias and the index all in one step. But that is more complicated....
        AliasOrIndex source = clusterService.state().metaData().getAliasAndIndexLookup().get(task.getRequest().getSourceIndex());
        IndicesAliasesRequest aliases = new IndicesAliasesRequest();
        for (Alias alias: task.getRequest().getCreateIndexRequest().aliases()) {
            AliasAction aliasAction = new AliasAction(AliasAction.Type.ADD, task.getRequest().getCreateIndexRequest().index(),
                    alias.name());
            aliasAction.filter(alias.filter());
            aliasAction.searchRouting(alias.searchRouting());
            aliasAction.indexRouting(alias.indexRouting());
            aliases.addAliasAction(aliasAction);
        }
        // Strip all the aliases from the source indexes while we add aliases to the new index.
        for (IndexMetaData indexMetaData : source.getIndices()) {
            for (ObjectCursor<String> alias : indexMetaData.getAliases().keys()) {
                aliases.addAliasAction(new AliasAction(AliasAction.Type.ADD, indexMetaData.getIndex().getName(), alias.value));
            }
        }
        aliases.timeout(task.getRequest().timeout());
        client.admin().indices().aliases(aliases, new ActionListener<IndicesAliasesResponse>() {
            @Override
            public void onResponse(IndicesAliasesResponse response) {
                if (false == response.isAcknowledged()) {
                    throw new ElasticsearchException("Timed out waiting to remove aliases");
                }
                removedIndex(task);
            }

            @Override
            public void onFailure(Exception e) {
                task.getListener().onFailure(e);
            }
        });
    }

    void removedIndex(MigrateIndexTask task) {
        DeleteIndexRequest delete = new DeleteIndexRequest(task.getRequest().getSourceIndex());
        delete.timeout(task.getRequest().timeout());
        client.admin().indices().delete(delete, new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse response) {
                if (false == response.isAcknowledged()) {
                    throw new ElasticsearchException("Timed out deleting [" + task.getRequest().getSourceIndex() + "]");
                }
                finishHim(task, new MigrateIndexResponse(true, false));
            }

            @Override
            public void onFailure(Exception e) {
                task.getListener().onFailure(e);
            }
        });
    }

    void finishHim(MigrateIndexTask task, MigrateIndexResponse response) {
        task.getListener().onResponse(response);
    }
}
