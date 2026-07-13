/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.CircuitBreakerPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.ql.plugin.QlPlugin;

import java.nio.file.Path;

public class LocalStateEQLXPackPlugin extends LocalStateCompositeXPackPlugin implements CircuitBreakerPlugin {

    private final EqlPlugin eqlPlugin;

    public LocalStateEQLXPackPlugin(final Settings settings, final Path configPath) {
        super(settings, configPath);
        LocalStateEQLXPackPlugin thisVar = this;
        this.eqlPlugin = new EqlPlugin() {
            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        };
        plugins.add(eqlPlugin);
        plugins.add(new QlPlugin());
    }

    @Override
    public BreakerSettings getCircuitBreaker(Settings settings) {
        return eqlPlugin.getCircuitBreaker(settings);
    }

    @Override
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        eqlPlugin.setCircuitBreaker(circuitBreaker);
    }
}
