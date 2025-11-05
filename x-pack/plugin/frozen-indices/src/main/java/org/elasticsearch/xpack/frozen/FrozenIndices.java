/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.frozen;

import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;

import java.util.ArrayList;
import java.util.List;

/**
 * Plugin for frozen indices functionality in Elasticsearch.
 * <p>
 * This plugin provides support for frozen indices, which are read-only indices optimized
 * for reduced memory footprint. It registers the necessary transport actions for tracking
 * frozen indices usage statistics.
 * </p>
 */
public class FrozenIndices extends Plugin implements ActionPlugin {

    /**
     * Returns the list of action handlers provided by this plugin.
     * <p>
     * Registers the {@link FrozenIndicesUsageTransportAction} for tracking usage
     * statistics of the frozen indices feature.
     * </p>
     *
     * @return a list of action handlers for frozen indices operations
     */
    @Override
    public List<ActionHandler> getActions() {
        List<ActionHandler> actions = new ArrayList<>();
        actions.add(new ActionHandler(XPackUsageFeatureAction.FROZEN_INDICES, FrozenIndicesUsageTransportAction.class));
        return actions;
    }
}
