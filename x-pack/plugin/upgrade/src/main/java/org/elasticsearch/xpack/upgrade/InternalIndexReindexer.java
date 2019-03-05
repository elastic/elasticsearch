/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

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
import org.elasticsearch.script.Script;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportResponse;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.index.IndexSettings.same;

/**
 * A component that performs the following upgrade procedure:
 * <p>
 * - Check that all data and master nodes are running running the same version
 * - Create a new index .{name}-6
 * - Make index .{name} read only
 * - Reindex from .{name} to .{name}-6 with transform
 * - Delete index .{name} and add alias .{name} to .{name}-6
 */
public class InternalIndexReindexer<T> {

    private final Client client;
    private final ClusterService clusterService;
    private final Script transformScript;
    private final String[] types;
    private final int version;
    private final Consumer<ActionListener<T>> preUpgrade;
    private final BiConsumer<T, ActionListener<TransportResponse.Empty>> postUpgrade;

    public InternalIndexReindexer(Client client, ClusterService clusterService, int version, Script transformScript, String[] types,
                                  Consumer<ActionListener<T>> preUpgrade,
                                  BiConsumer<T, ActionListener<TransportResponse.Empty>> postUpgrade) {
        this.client = client;
        this.clusterService = clusterService;
        this.transformScript = transformScript;
        this.types = types;
        this.version = version;
        this.preUpgrade = preUpgrade;
        this.postUpgrade = postUpgrade;
    }

    public void upgrade(TaskId task, String index, ClusterState clusterState, ActionListener<BulkByScrollResponse> listener) {
        ParentTaskAssigningClient parentAwareClient = new ParentTaskAssigningClient(client, task);
        preUpgrade.accept(ActionListener.wrap(
                t -> innerUpgrade(parentAwareClient, index, clusterState, ActionListener.wrap(
                        response -> postUpgrade.accept(t, ActionListener.wrap(
                                empty -> listener.onResponse(response),
                                listener::onFailure
                        )),
                        listener::onFailure
                )),
                listener::onFailure));
    }

    private void innerUpgrade(ParentTaskAssigningClient parentAwareClient, String index, ClusterState clusterState,
                              ActionListener<BulkByScrollResponse> listener) {
        String newIndex = index + "-" + version;
        try {
            checkMasterAndDataNodeVersion(clusterState);
            parentAwareClient.admin().indices().prepareCreate(newIndex).execute(ActionListener.wrap(createIndexResponse ->
                    setReadOnlyBlock(index, ActionListener.wrap(setReadOnlyResponse ->
                            reindex(parentAwareClient, index, newIndex, ActionListener.wrap(
                                    bulkByScrollResponse -> // Successful completion of reindexing - delete old index
                                            removeReadOnlyBlock(parentAwareClient, index, ActionListener.wrap(unsetReadOnlyResponse ->
                                                    parentAwareClient.admin().indices().prepareAliases().removeIndex(index)
                                                            .addAlias(newIndex, index).execute(ActionListener.wrap(deleteIndexResponse ->
                                                            listener.onResponse(bulkByScrollResponse), listener::onFailure
                                                    )), listener::onFailure
                                            )),
                                    e -> // Something went wrong during reindexing - remove readonly flag and report the error
                                            removeReadOnlyBlock(parentAwareClient, index, ActionListener.wrap(unsetReadOnlyResponse -> {
                                                listener.onFailure(e);
                                            }, e1 -> {
                                                listener.onFailure(e);
                                            }))
                            )), listener::onFailure
                    )), listener::onFailure
            ));
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

    private void reindex(ParentTaskAssigningClient parentAwareClient, String index, String newIndex,
                         ActionListener<BulkByScrollResponse> listener) {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(index);
        reindexRequest.setSourceDocTypes(types);
        reindexRequest.setDestIndex(newIndex);
        reindexRequest.setRefresh(true);
        reindexRequest.setScript(transformScript);
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

}
