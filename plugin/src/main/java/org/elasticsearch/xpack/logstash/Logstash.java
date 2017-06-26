/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.logstash;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.InternalClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class activates/deactivates the logstash modules depending if we're running a node client or transport client
 */
public class Logstash implements ActionPlugin {

    public static final String NAME = "logstash";

    private final Settings settings;
    private final boolean enabled;
    private final boolean transportClientMode;

    public Logstash(Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.LOGSTASH_ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    boolean isEnabled() {
      return enabled;
    }

    boolean isTransportClient() {
      return transportClientMode;
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(b -> {
            XPackPlugin.bindFeatureSet(b, LogstashFeatureSet.class);
        });
        return modules;
    }

    public Collection<Object> createComponents(InternalClient client, ClusterService clusterService) {
        if (this.transportClientMode || enabled == false) {
            return Collections.emptyList();
        }

        return Collections.singletonList(new LogstashTemplateRegistry(settings, clusterService, client));
    }
}
