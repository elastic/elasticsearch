/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.plugin;

import org.elasticsearch.alerts.AlertManager;
import org.elasticsearch.alerts.scheduler.AlertScheduler;
import org.elasticsearch.alerts.AlertingModule;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;

public class AlertsPlugin extends AbstractPlugin {

    @Override public String name() {
        return "alerts";
    }

    @Override public String description() {
        return "Elasticsearch Alerts";
    }

    @Override
    public Collection<java.lang.Class<? extends LifecycleComponent>> services() {
        Collection<java.lang.Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        services.add(AlertManager.class);
        services.add(AlertScheduler.class);
        return services;
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(AlertingModule.class);
        return modules;
    }
}
