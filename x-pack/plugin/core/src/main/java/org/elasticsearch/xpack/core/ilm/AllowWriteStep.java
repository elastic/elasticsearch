/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import java.util.Objects;

/**
 * Make shrunken index writable, if requested, by removing {@link IndexMetadata.APIBlock#WRITE } block on the index.
 */
public class AllowWriteStep extends UpdateSettingsStep {
    public static final String NAME = "allow-write-after-shrink";

    public static final Settings CLEAR_BLOCKS_WRITE_SETTING = Settings.builder()
        .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), (String) null)
        .build();

    private final boolean allowWriteAfterShrink;
    public AllowWriteStep(StepKey key, StepKey nextStepKey, Client client, boolean allowWriteAfterShrink) {
        super(key, nextStepKey, client, CLEAR_BLOCKS_WRITE_SETTING);
        this.allowWriteAfterShrink = allowWriteAfterShrink;
    }

    public boolean isAllowWriteAfterShrink() {
        return allowWriteAfterShrink;
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ClusterState currentState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        if (allowWriteAfterShrink) {
            super.performAction(indexMetadata, currentState, observer, listener);
        } else {
            listener.onResponse(null);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        AllowWriteStep that = (AllowWriteStep) o;
        return allowWriteAfterShrink == that.allowWriteAfterShrink;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), allowWriteAfterShrink);
    }
}
