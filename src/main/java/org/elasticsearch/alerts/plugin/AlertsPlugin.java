/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.plugin;

import org.elasticsearch.alerts.AlertingModule;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class AlertsPlugin extends AbstractPlugin {

    public static final String THREAD_POOL_NAME = "alerts";

    @Override public String name() {
        return THREAD_POOL_NAME;
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
    public Settings additionalSettings() {
        return settingsBuilder()
                .put("threadpool."+ THREAD_POOL_NAME + ".type","cached")
                .build();
    }


}
