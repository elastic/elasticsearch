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
import org.elasticsearch.common.settings.Settings;

import java.util.function.LongSupplier;

/**
 * An Implementation of {@link IndexLifecycleContext} which writes lifecycle
 * state to index settings.
 */
public class InternalIndexLifecycleContext implements IndexLifecycleContext {

    private Client client;
    private IndexMetaData idxMeta;
    private LongSupplier nowSupplier;

    /**
     * @param idxMeta
     *            the {@link IndexMetaData} for the index.
     * @param client
     *            the {@link Client} to use when modifying the index settings.
     * @param nowSupplier
     *            a {@link LongSupplier} to provide the current timestamp when
     *            required.
     */
    public InternalIndexLifecycleContext(IndexMetaData idxMeta, Client client, LongSupplier nowSupplier) {
        this.idxMeta = idxMeta;
        this.client = client;
        this.nowSupplier = nowSupplier;
    }

    @Override
    public void setPhase(String phase, Listener listener) {
        writeSettings(idxMeta.getIndex().getName(),
                Settings.builder().put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), phase)
                .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build(), listener);
    }

    @Override
    public String getPhase() {
        return IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.get(idxMeta.getSettings());
    }

    @Override
    public void setAction(String action, Listener listener) {
        writeSettings(idxMeta.getIndex().getName(),
                Settings.builder().put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), action).build(), listener);
    }

    @Override
    public String getAction() {
        return IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.get(idxMeta.getSettings());
    }

    @Override
    public String getLifecycleTarget() {
        return idxMeta.getIndex().getName();
    }

    @Override
    public int getNumberOfReplicas() {
        return idxMeta.getNumberOfReplicas();
    }

    @Override
    public boolean canExecute(Phase phase) {
        long now = nowSupplier.getAsLong();
        long indexCreated = idxMeta.getCreationDate();
        return (indexCreated + phase.getAfter().millis()) <= now;
    }

    @Override
    public void executeAction(LifecycleAction action, LifecycleAction.Listener listener) {
        action.execute(idxMeta.getIndex(), client, listener);
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
