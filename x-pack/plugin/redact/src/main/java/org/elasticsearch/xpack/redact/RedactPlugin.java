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

public class RedactPlugin extends Plugin implements IngestPlugin {

    private final Settings settings;

    public RedactPlugin(final Settings settings) {
        this.settings = settings;
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(RedactProcessor.TYPE, new RedactProcessor.Factory(getLicenseState(), parameters.matcherWatchdog));
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }
}
