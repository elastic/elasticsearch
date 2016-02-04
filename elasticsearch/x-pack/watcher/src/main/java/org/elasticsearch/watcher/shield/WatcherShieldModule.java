/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.shield;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authz.privilege.ClusterPrivilege;

/**
 *
 */
public class WatcherShieldModule extends AbstractModule {

    private final ESLogger logger;

    public WatcherShieldModule(Settings settings) {
        this.logger = Loggers.getLogger(WatcherShieldModule.class, settings);
        if (ShieldPlugin.shieldEnabled(settings)) {
            registerClusterPrivilege("manage_watcher", "cluster:admin/watcher/*", "cluster:monitor/watcher/*");
            registerClusterPrivilege("monitor_watcher", "cluster:monitor/watcher/*");
        }
    }

    void registerClusterPrivilege(String name, String... patterns) {
        try {
            ClusterPrivilege.addCustom(name, patterns);
        } catch (Exception se) {
            logger.warn("could not register cluster privilege [{}]", name);

            // we need to prevent bubbling the shield exception here for the tests. In the tests
            // we create multiple nodes in the same jvm and since the custom cluster is a static binding
            // multiple nodes will try to add the same privileges multiple times.
        }
    }

    @Override
    protected void configure() {
    }
}
