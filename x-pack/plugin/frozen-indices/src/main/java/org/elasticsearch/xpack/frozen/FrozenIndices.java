/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.frozen;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.engine.frozen.FrozenEngine;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.frozen.action.TransportFreezeIndexAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FrozenIndices extends Plugin implements ActionPlugin {

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(FrozenEngine.INDEX_FROZEN);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(XPackUsageFeatureAction.FROZEN_INDICES, FrozenIndicesUsageTransportAction.class));
        actions.add(new ActionHandler<>(FreezeIndexAction.INSTANCE, TransportFreezeIndexAction.class));
        return actions;
    }
}
