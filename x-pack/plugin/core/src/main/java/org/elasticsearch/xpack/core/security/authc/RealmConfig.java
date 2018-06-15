/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;

import java.util.Collection;
import java.util.Collections;

public class RealmConfig {

    final String name;
    final boolean enabled;
    final int order;
    private final String type;
    private final Collection<String> lookupRealmNames;
    final Settings settings;

    private final Environment env;
    private final Settings globalSettings;
    private final ThreadContext threadContext;

    public RealmConfig(String name, Settings settings, Settings globalSettings, Environment env,
                       ThreadContext threadContext) {
        this.name = name;
        this.settings = settings;
        this.globalSettings = globalSettings;
        this.env = env;
        enabled = RealmSettings.ENABLED_SETTING.get(settings);
        order = RealmSettings.ORDER_SETTING.get(settings);
        type = RealmSettings.TYPE_SETTING.get(settings);
        this.threadContext = threadContext;
        this.lookupRealmNames = Collections.unmodifiableCollection(RealmSettings.LOOKUP_REALMS_SETTING.get(settings));
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

    public String type() {
        return type;
    }

    public Collection<String> lookupRealms() {
        return lookupRealmNames;
    }

    public Settings settings() {
        return settings;
    }

    public Settings globalSettings() {
        return globalSettings;
    }

    public Logger logger(Class clazz) {
        return Loggers.getLogger(clazz, globalSettings);
    }

    public Environment env() {
        return env;
    }

    public ThreadContext threadContext() {
        return threadContext;
    }
}
