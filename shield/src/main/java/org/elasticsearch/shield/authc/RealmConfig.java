/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

/**
 *
 */
public class RealmConfig {

    final String name;
    final boolean enabled;
    final int order;
    final Settings settings;

    private final Environment env;
    private final Settings globalSettings;

    public RealmConfig(String name, Settings settings, Settings globalSettings) {
        this(name, settings, globalSettings, new Environment(globalSettings));
    }

    public RealmConfig(String name, Settings settings, Settings globalSettings, Environment env) {
        this.name = name;
        this.settings = settings;
        this.globalSettings = globalSettings;
        this.env = env;
        enabled = settings.getAsBoolean("enabled", true);
        order = settings.getAsInt("order", Integer.MAX_VALUE);
    }
    
    public String name() {
        return name;
    }

    public boolean enabled() {
        return enabled;
    }
    
    public int order() {
        return order;
    }

    public Settings settings() {
        return settings;
    }

    public Settings globalSettings() {
        return globalSettings;
    }

    public ESLogger logger(Class clazz) {
        return Loggers.getLogger(clazz, globalSettings);
    }

    public Environment env() {
        return env;
    }
}
