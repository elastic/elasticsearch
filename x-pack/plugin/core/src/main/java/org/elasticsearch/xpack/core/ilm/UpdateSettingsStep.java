/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Updates the settings for an index.
 */
public class UpdateSettingsStep extends AsyncActionStep {
    public static final String NAME = "update-settings";

    private static final BiFunction<String, LifecycleExecutionState, String> DEFAULT_TARGET_INDEX_NAME_SUPPLIER = (index, state) -> index;

    private final BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier;
    private final Function<IndexMetadata, Settings> settingsSupplier;

    /**
     * Use this constructor when you want to update the index that ILM runs on with <i>constant</i> settings.
     */
    public UpdateSettingsStep(StepKey key, StepKey nextStepKey, Client client, Settings settings) {
        this(key, nextStepKey, client, DEFAULT_TARGET_INDEX_NAME_SUPPLIER, indexMetadata -> settings);
    }

    /**
     * Use this constructor when you want to update an index other than the one ILM runs on, and/or when you have non-constant settings
     * (i.e., settings that depend on the index metadata).
     */
    public UpdateSettingsStep(
        StepKey key,
        StepKey nextStepKey,
        Client client,
        BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier,
        Function<IndexMetadata, Settings> settingsSupplier
    ) {
        super(key, nextStepKey, client);
        this.targetIndexNameSupplier = targetIndexNameSupplier;
        this.settingsSupplier = settingsSupplier;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ProjectState currentState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        String indexName = targetIndexNameSupplier.apply(indexMetadata.getIndex().getName(), indexMetadata.getLifecycleExecutionState());
        Settings settings = settingsSupplier.apply(indexMetadata);
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName).masterNodeTimeout(TimeValue.MAX_VALUE)
            .settings(settings);
        getClient(currentState.projectId()).admin()
            .indices()
            .updateSettings(updateSettingsRequest, listener.delegateFailureAndWrap((l, r) -> l.onResponse(null)));
    }

    public Function<IndexMetadata, Settings> getSettingsSupplier() {
        return settingsSupplier;
    }
}
