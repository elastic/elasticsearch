/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.graph;

import static org.elasticsearch.common.settings.Setting.Scope.CLUSTER;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.graph.action.GraphExploreAction;
import org.elasticsearch.graph.action.TransportGraphExploreAction;
import org.elasticsearch.graph.license.GraphLicensee;
import org.elasticsearch.graph.license.GraphModule;
import org.elasticsearch.graph.rest.action.RestGraphAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.action.SearchTransportService;
import org.elasticsearch.shield.Shield;
import org.elasticsearch.xpack.XPackPlugin;

public class Graph extends Plugin {

    public static final String NAME = "graph";
    private final boolean transportClientMode;
    protected final boolean enabled;
    
    
    public Graph(Settings settings) {
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        enabled = enabled(settings);
        // adding the graph privileges to shield
        if (Shield.enabled(settings)) {
            Shield.registerIndexPrivilege( "graph", GraphExploreAction.NAME, SearchTransportService.QUERY_ACTION_NAME, 
                    SearchAction.NAME, SearchTransportService.QUERY_FETCH_ACTION_NAME);
        }        
    }    
    
    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Elasticsearch Graph Plugin";
    }
    
    public static boolean enabled(Settings settings) {
        return XPackPlugin.featureEnabled(settings, NAME, true);
    }    
    
    public void onModule(ActionModule actionModule) {
        if (enabled) {
            actionModule.registerAction(GraphExploreAction.INSTANCE, TransportGraphExploreAction.class);
        }
    }

    public void onModule(NetworkModule module) {
        if (enabled && transportClientMode == false) {
            module.registerRestHandler(RestGraphAction.class);        
        }
    }    
    
    public void onModule(SettingsModule module) {
        module.registerSetting(Setting.boolSetting(XPackPlugin.featureEnabledSetting(NAME), true, false, CLUSTER));
    }    
    

    public Collection<Module> nodeModules() {
        if (enabled == false|| transportClientMode) {
            return Collections.emptyList();
        }
        return Arrays.<Module> asList(new GraphModule());
    }
    
    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (enabled == false|| transportClientMode) {
            return Collections.emptyList();
        }
        return Arrays.<Class<? extends LifecycleComponent>>asList(
            GraphLicensee.class
          );
    }      
    
}
