/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.redact;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Map;

/**
 * Plugin for data redaction functionality in Elasticsearch ingest pipelines.
 * <p>
 * This plugin provides an ingest processor that can redact sensitive information
 * from documents during ingestion. The redaction processor uses pattern matching
 * to identify and replace sensitive data such as credit card numbers, email addresses,
 * or custom patterns.
 * </p>
 */
public class RedactPlugin extends Plugin implements IngestPlugin {

    private final Settings settings;

    /**
     * Constructs a new RedactPlugin with the specified settings.
     *
     * @param settings the node settings
     */
    public RedactPlugin(final Settings settings) {
        this.settings = settings;
    }

    /**
     * Returns the ingest processors provided by this plugin.
     * <p>
     * Registers the {@link RedactProcessor} which can be used in ingest pipelines
     * to redact sensitive information from documents. The processor is license-aware
     * and uses the matcher watchdog to prevent runaway regex operations.
     * </p>
     *
     * @param parameters the processor parameters including the matcher watchdog
     * @return a map containing the redact processor factory
     */
    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(RedactProcessor.TYPE, new RedactProcessor.Factory(getLicenseState(), parameters.matcherWatchdog));
    }

    /**
     * Returns the X-Pack license state.
     * <p>
     * The redact processor may require specific license levels to operate.
     * </p>
     *
     * @return the shared X-Pack license state
     */
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }
}
