/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.upgrade;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportResponse;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.index.IndexSettings.same;

public class InternalSplitReindexer {

    private final Client client;
    private final ClusterService clusterService;
    private final String sourceIndex;
    private final String sourceType;
    private final Consumer<ActionListener<TransportResponse.Empty>> postUpgrade;
    private final Destination[] destinations;

    public InternalSplitReindexer(Client client, ClusterService clusterService, String sourceIndex, String sourceType,
            Consumer<ActionListener<TransportResponse.Empty>> postUpgrade, Destination... destinations) {
        this.client = client;
        this.clusterService = clusterService;
        this.sourceIndex = sourceIndex;
        this.sourceType = sourceType;
        this.postUpgrade = postUpgrade;
        this.destinations = destinations;
    }

    public void upgrade(TaskId task, ActionListener<BulkByScrollResponse> listener) {
        final ParentTaskAssigningClient parentAwareClient = new ParentTaskAssigningClient(client, task);
        final ClusterState clusterState = clusterService.state();
        final CountDownLatch countDown = new CountDownLatch(destinations.length);
        final List<Exception> reindexExceptions = new CopyOnWriteArrayList<>();
        setReadOnlyBlock(sourceIndex, ActionListener.wrap(setReadOnlyResponse -> {
            for (Destination destination : destinations) {
                reindex(parentAwareClient, sourceIndex, sourceType, destination, ActionListener.wrap(bulkByScrollResponse -> {
                    countDown.countDown();
                    destination.postUpgrade
                    if (co)
                }, e -> {
                    countDown.countDown();
                    reindexExceptions.add(e);
                }));
            }
        }, listener::onFailure));
        
        innerUpgrade(parentAwareClient, ActionListener.wrap(
                        response -> postUpgrade.accept(ActionListener.wrap(
                                empty -> removeReadOnlyBlock(parentAwareClient, sourceIndex, A listener.onResponse(response), listener::onFailure)),
                        listener::onFailure));
    }

    private void innerUpgrade(ParentTaskAssigningClient parentAwareClient, ActionListener<BulkByScrollResponse> listener) {
        final ClusterState clusterState = clusterService.state();
        final CountDownLatch countDown = new CountDownLatch(destinations.length);
        final SetOnce<Exception> exception = new SetOnce<>();
        try {
            setReadOnlyBlock(sourceIndex, ActionListener.wrap(setReadOnlyResponse -> {
                for (Destination destination : destinations) {
                    reindex(parentAwareClient, sourceIndex, sourceType, destination, ActionListener.wrap(bulkByScrollResponse -> {
                        countDown.countDown();
                    }, e -> {
                        countDown.countDown();
                    }));
                }
            }, e -> listener.onFailure(e)));
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
        
        try {
            final ClusterState clusterState = clusterService.state();
            final CountDownLatch countDown = new CountDownLatch(destinations.length);
            checkMasterAndDataNodeVersion(clusterState);
            for (Destination destination : destinations) {
                setReadOnlyBlock(sourceIndex, ActionListener.wrap(setReadOnlyResponse ->
                reindex(parentAwareClient, sourceIndex, sourceType, destination, ActionListener.wrap(bulkByScrollResponse ->
                    // Successful completion of reindexing
                    removeReadOnlyBlock(parentAwareClient, sourceIndex, ActionListener.wrap(unsetReadOnlyResponse ->
                        parentAwareClient.admin().indices().prepareAliases()
                        .removeIndex(oldIndex)
                        .removeAlias(oldIndex, alias)
                        .addAlias(newIndex, alias)
                        .execute(ActionListener.wrap(deleteIndexResponse ->
                        listener.onResponse(bulkByScrollResponse),
                        listener::onFailure)),
                        listener::onFailure)),
                        e -> // Something went wrong during reindexing - remove readonly flag and report the error
                        removeReadOnlyBlock(parentAwareClient, sourceIndex, ActionListener.wrap(unsetReadOnlyResponse -> {
                            listener.onFailure(e);
                        }, e1 -> {
                            listener.onFailure(e);
                        }))
                )),
                listener::onFailure
                 ));
                
            }
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    private void checkMasterAndDataNodeVersion(ClusterState clusterState) {
        if (clusterState.nodes().getMinNodeVersion().before(Upgrade.UPGRADE_INTRODUCED)) {
            throw new IllegalStateException("All nodes should have at least version [" + Upgrade.UPGRADE_INTRODUCED + "] to upgrade");
        }
    }

    private void removeReadOnlyBlock(ParentTaskAssigningClient parentAwareClient, String index,
                                     ActionListener<AcknowledgedResponse> listener) {
        Settings settings = Settings.builder().put(IndexMetaData.INDEX_READ_ONLY_SETTING.getKey(), false).build();
        parentAwareClient.admin().indices().prepareUpdateSettings(index).setSettings(settings).execute(listener);
    }

    private void reindex(ParentTaskAssigningClient parentAwareClient, String sourceIndex, String sourceType, Destination destination,
            ActionListener<BulkByScrollResponse> listener) {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndex);
        reindexRequest.setSourceDocTypes(sourceType);
        reindexRequest.setDestIndex(destination.index);
        reindexRequest.setRefresh(true);
        reindexRequest.setSourceQuery(destination.query);
        parentAwareClient.execute(ReindexAction.INSTANCE, reindexRequest, listener);
    }

    /**
     * Makes the index readonly if it's not set as a readonly yet
     */
    private void setReadOnlyBlock(String index, ActionListener<TransportResponse.Empty> listener) {
        clusterService.submitStateUpdateTask("lock-index-for-upgrade", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                final IndexMetaData indexMetaData = currentState.metaData().index(index);
                if (indexMetaData == null) {
                    throw new IndexNotFoundException(index);
                }

                if (indexMetaData.getState() != IndexMetaData.State.OPEN) {
                    throw new IllegalStateException("unable to upgrade a closed index[" + index + "]");
                }
                if (currentState.blocks().hasIndexBlock(index, IndexMetaData.INDEX_READ_ONLY_BLOCK)) {
                    throw new IllegalStateException("unable to upgrade a read-only index[" + index + "]");
                }

                final Settings indexSettingsBuilder =
                        Settings.builder()
                                .put(indexMetaData.getSettings())
                                .put(IndexMetaData.INDEX_READ_ONLY_SETTING.getKey(), true)
                                .build();
                final IndexMetaData.Builder builder = IndexMetaData.builder(indexMetaData).settings(indexSettingsBuilder);
                assert same(indexMetaData.getSettings(), indexSettingsBuilder) == false;
                builder.settingsVersion(1 + builder.settingsVersion());

                MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData()).put(builder);

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks())
                        .addIndexBlock(index, IndexMetaData.INDEX_READ_ONLY_BLOCK);

                return ClusterState.builder(currentState).metaData(metaDataBuilder).blocks(blocks).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(TransportResponse.Empty.INSTANCE);
            }
        });
    }

    static class Destination {

        private final String index;
        private final QueryBuilder query;
        private final Consumer<ActionListener<TransportResponse.Empty>> postUpgrade;

        Destination(String index, QueryBuilder query, Consumer<ActionListener<TransportResponse.Empty>> postUpgrade) {
            this.index = index;
            this.query = query;
            this.postUpgrade = postUpgrade;
        }
    }

}
