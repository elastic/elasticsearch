/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.function.LongSupplier;

/**
 * An Implementation of {@link IndexLifecycleContext} which writes lifecycle
 * state to index settings.
 */
public class InternalIndexLifecycleContext implements IndexLifecycleContext {

    private Client client;
    private Index index;
    private LongSupplier nowSupplier;
    private ClusterService clusterService;

    /**
     * @param index
     *            the {@link Index} for this context.
     * @param client
     *            the {@link Client} to use when modifying the index settings.
     * @param nowSupplier
     *            a {@link LongSupplier} to provide the current timestamp when
     *            required.
     */
    public InternalIndexLifecycleContext(Index index, Client client, ClusterService clusterService, LongSupplier nowSupplier) {
        this.index = index;
        this.client = client;
        this.clusterService = clusterService;
        this.nowSupplier = nowSupplier;
    }

    private IndexMetaData getIdxMetaData() {
        return clusterService.state().metaData().index(index.getName());
    }

    @Override
    public void setPhase(String phase, Listener listener) {
        Settings newLifecyclePhaseSettings = Settings.builder()
            .put(IndexLifecycle.LIFECYCLE_PHASE_SETTING.getKey(), phase)
            .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), "").build();
        writeSettings(index.getName(), newLifecyclePhaseSettings, listener);
    }

    @Override
    public String getPhase() {
        return IndexLifecycle.LIFECYCLE_PHASE_SETTING.get(getIdxMetaData().getSettings());
    }

    @Override
    public void setAction(String action, Listener listener) {
        Settings newLifecycleActionSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), action).build();
        writeSettings(index.getName(), newLifecycleActionSettings, listener);
    }

    @Override
    public String getAction() {
        return IndexLifecycle.LIFECYCLE_ACTION_SETTING.get(getIdxMetaData().getSettings());
    }

    @Override
    public String getLifecycleTarget() {
        return index.getName();
    }

    @Override
    public int getNumberOfReplicas() {
        return getIdxMetaData().getNumberOfReplicas();
    }

    @Override
    public boolean canExecute(Phase phase) {
        long now = nowSupplier.getAsLong();
        long indexCreated = getIdxMetaData().getCreationDate();
        return (indexCreated + phase.getAfter().millis()) <= now;
    }

    @Override
    public void executeAction(LifecycleAction action, LifecycleAction.Listener listener) {
        action.execute(index, client, clusterService, listener);
    }

    private void writeSettings(String index, Settings settings, Listener listener) {
        client.admin().indices().updateSettings(new UpdateSettingsRequest(settings, index), new ActionListener<UpdateSettingsResponse>() {

            @Override
            public void onResponse(UpdateSettingsResponse response) {
                if (response.isAcknowledged()) {
                    listener.onSuccess();
                } else {
                    listener.onFailure(null);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

}
