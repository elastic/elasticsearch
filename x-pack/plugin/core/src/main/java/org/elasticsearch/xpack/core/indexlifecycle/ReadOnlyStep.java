/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Objects;

public class ReadOnlyStep extends ClusterStateActionStep {
    public static final String NAME = "read-only";

    ReadOnlyStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        return ClusterState.builder(clusterState).metaData(MetaData.builder(clusterState.metaData())
            .updateSettings(Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true).build(),
                index.getName())).build();
    }
}
