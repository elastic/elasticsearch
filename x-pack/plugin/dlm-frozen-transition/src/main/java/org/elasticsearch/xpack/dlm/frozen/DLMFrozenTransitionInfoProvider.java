/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexFrozenTransition;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.datastreams.lifecycle.FrozenTransitionInfoProvider;

/**
 * Exposes the {@link DLMFrozenTransitionExecutor}'s per-index transition status to the data stream lifecycle
 * explain API. Registered as a {@link FrozenTransitionInfoProvider} extension via SPI; the single-argument
 * constructor is required so {@code PluginsService} can instantiate this extension with the owning plugin
 * before that plugin's components (including the executor) are created.
 */
public class DLMFrozenTransitionInfoProvider implements FrozenTransitionInfoProvider {

    private final DLMFrozenTransitionPlugin plugin;

    public DLMFrozenTransitionInfoProvider(DLMFrozenTransitionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean infoAvailable() {
        return true;
    }

    @Override
    public ExplainIndexFrozenTransition.Status getTransitionStatus(ProjectId projectId, String indexName) {
        DLMFrozenTransitionExecutor executor = plugin.getTransitionExecutor();
        return executor == null ? ExplainIndexFrozenTransition.Status.NOT_STARTED : executor.getTransitionStatus(projectId, indexName);
    }
}
