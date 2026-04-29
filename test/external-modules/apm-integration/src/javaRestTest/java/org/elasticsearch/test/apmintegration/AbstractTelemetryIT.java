/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;

public abstract class AbstractTelemetryIT extends ESRestTestCase {
    private static final Logger logger = LogManager.getLogger(AbstractTelemetryIT.class);

    /**
     * The APM agent is reconfigured dynamically by the APM module after booting,
     * and the agent only reloads its configuration every 30 seconds.
     * The first telemetry can be blocked waiting for this, so let's give it
     * a good long time before giving up.
     * <p>
     * This should be unnecessary when the APM agent is no longer used.
     */
    static final int TELEMETRY_TIMEOUT = 40;

    /**
     * Concrete subclasses supply their own {@link RecordingApmServer} static field
     * and expose it here so the shared test methods can register consumers and clear state before each test.
     * <p>
     * The cluster captures the URL of the {@link RecordingApmServer} at startup,
     * so they must be paired one-to-one, meaning their scopes and lifetimes must coincide.
     * <p>
     * Mirrors the {@link #getTestRestCluster()} pattern.
     */
    protected abstract RecordingApmServer apmServer();

    @Before
    public void resetApmServer() {
        apmServer().reset();
    }

    /**
     * Utility for concrete subclasses to establish their {@link RecordingApmServer} and {@link ElasticsearchCluster}.
     */
    protected static TestRule buildRuleChain(RecordingApmServer server, ElasticsearchCluster cluster) {
        return RuleChain.outerRule(server).around(cluster).around((base, description) -> new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } finally {
                    try {
                        closeClients();
                    } catch (IOException e) {
                        logger.error("failed to close REST clients after test", e);
                    }
                }
            }
        });
    }
}
