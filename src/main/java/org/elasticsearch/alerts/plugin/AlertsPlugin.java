/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.plugin;

import org.elasticsearch.alerts.AlertingModule;
import org.elasticsearch.alerts.support.init.InitializingService;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class AlertsPlugin extends AbstractPlugin {

    public static final String ALERT_THREAD_POOL_NAME = "alerts";
    public static final String SCHEDULER_THREAD_POOL_NAME = "alerts_scheduler";

    @Override public String name() {
        return ALERT_THREAD_POOL_NAME;
    }

    @Override public String description() {
        return "Elasticsearch Alerts";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(AlertingModule.class);
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        return ImmutableList.<Class<? extends LifecycleComponent>>of(
                // the initialization service must be first in the list
                // as other services may depend on one of the initialized
                // constructs
                InitializingService.class);
    }

    @Override
    public Settings additionalSettings() {
        return settingsBuilder()
                .put("threadpool." + ALERT_THREAD_POOL_NAME + ".type", "fixed")
                .put("threadpool." + ALERT_THREAD_POOL_NAME + ".size", 32) // Executing an alert involves a lot of wait time for networking (search, several index requests + optional trigger logic)
                .put("threadpool." + SCHEDULER_THREAD_POOL_NAME + ".type", "cached")
                .build();
    }


}
