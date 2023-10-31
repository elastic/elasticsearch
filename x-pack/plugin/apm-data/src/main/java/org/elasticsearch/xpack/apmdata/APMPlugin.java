/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apmdata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

public class APMPlugin extends Plugin implements ActionPlugin {
    private static final Logger logger = LogManager.getLogger(APMPlugin.class);

    private final SetOnce<APMIndexTemplateRegistry> registry = new SetOnce<>();

    @Override
    public Collection<?> createComponents(PluginServices services) {
        registry.set(
            new APMIndexTemplateRegistry(
                services.environment().settings(),
                services.clusterService(),
                services.threadPool(),
                services.client(),
                services.xContentRegistry()
            )
        );
        APMIndexTemplateRegistry registryInstance = registry.get();
        logger.info("APM is {}", registryInstance.isEnabled() ? "enabled" : "disabled");
        registryInstance.initialize();
        return List.of(registryInstance);
    }

    @Override
    public void close() {
        registry.get().close();
    }
}
