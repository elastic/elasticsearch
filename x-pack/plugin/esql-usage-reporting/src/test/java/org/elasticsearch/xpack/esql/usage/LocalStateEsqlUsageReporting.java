/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.nio.file.Path;

/**
 * Runs {@link EsqlUsageReportingPlugin} in a test node: x-pack-core's shared SSL service is a static that nodes of a test cluster never
 * set, so the plugin takes the service of this node's composite plugin, as the other x-pack plugins do in their tests.
 */
public class LocalStateEsqlUsageReporting extends LocalStateCompositeXPackPlugin {

    private final EsqlUsageReportingPlugin usagePlugin;

    public LocalStateEsqlUsageReporting(final Settings settings, final Path configPath) {
        super(settings, configPath);
        LocalStateEsqlUsageReporting thisVar = this;
        this.usagePlugin = new EsqlUsageReportingPlugin() {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }
        };
        plugins.add(usagePlugin);
    }

    EsqlUsageReportingPlugin usagePlugin() {
        return usagePlugin;
    }
}
