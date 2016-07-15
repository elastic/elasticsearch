/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.monitoring.action.TransportMonitoringBulkAction;
import org.elasticsearch.xpack.monitoring.agent.AgentService;
import org.elasticsearch.xpack.monitoring.agent.collector.CollectorModule;
import org.elasticsearch.xpack.monitoring.agent.exporter.ExporterModule;
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;
import org.elasticsearch.xpack.monitoring.client.MonitoringClientModule;
import org.elasticsearch.xpack.monitoring.rest.action.RestMonitoringBulkAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * This class activates/deactivates the monitoring modules depending if we're running a node client, transport client or tribe client:
 * - node clients: all modules are binded
 * - transport clients: only action/transport actions are binded
 * - tribe clients: everything is disables by default but can be enabled per tribe cluster
 */
public class Monitoring implements ActionPlugin {

    public static final String NAME = "monitoring";

    private final Settings settings;
    private final boolean enabled;
    private final boolean transportClientMode;
    private final boolean tribeNode;

    public Monitoring(Settings settings) {
        this.settings = settings;
        this.enabled = enabled(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        this.tribeNode = XPackPlugin.isTribeNode(settings);
    }

    boolean isEnabled() {
        return enabled;
    }

    boolean isTransportClient() {
        return transportClientMode;
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(new MonitoringModule(enabled, transportClientMode));
        modules.add(new ExporterModule(settings));
        if (enabled && transportClientMode == false && tribeNode == false) {
            modules.add(new CollectorModule());
            modules.add(new MonitoringClientModule());
        }
        return modules;
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (enabled == false || transportClientMode || tribeNode) {
            return Collections.emptyList();
        }
        return Arrays.<Class<? extends LifecycleComponent>>asList(AgentService.class, CleanerService.class);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        if (false == enabled || tribeNode) {
            return emptyList();
        }
        return singletonList(new ActionHandler<>(MonitoringBulkAction.INSTANCE, TransportMonitoringBulkAction.class));
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        if (false == enabled || tribeNode) {
            return emptyList();
        }
        return singletonList(RestMonitoringBulkAction.class);
    }

    public static boolean enabled(Settings settings) {
        return MonitoringSettings.ENABLED.get(settings);
    }
}
