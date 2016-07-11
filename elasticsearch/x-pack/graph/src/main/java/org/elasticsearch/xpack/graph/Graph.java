/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.graph;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.graph.action.TransportGraphExploreAction;
import org.elasticsearch.xpack.graph.rest.action.RestGraphAction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class Graph extends Plugin implements ActionPlugin {

    public static final String NAME = "graph";
    private final boolean transportClientMode;
    protected final boolean enabled;
    
    
    public Graph(Settings settings) {
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        enabled = enabled(settings);
    }
    
    public static boolean enabled(Settings settings) {
        return XPackPlugin.featureEnabled(settings, NAME, true);
    }

    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(new GraphModule(enabled, transportClientMode));
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        if (enabled == false|| transportClientMode) {
            return Collections.emptyList();
        }
        return Collections.singletonList(GraphLicensee.class);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        if (false == enabled) {
            return emptyList();
        }
        return singletonList(new ActionHandler<>(GraphExploreAction.INSTANCE, TransportGraphExploreAction.class));
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        if (false == enabled) {
            return emptyList();
        }
        return singletonList(RestGraphAction.class);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(Setting.boolSetting(XPackPlugin.featureEnabledSetting(NAME), true, Setting.Property.NodeScope));
    }

}
