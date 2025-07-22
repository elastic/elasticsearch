/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.plugins;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logstashbridge.ingest.ProcessorBridge;
import org.elasticsearch.xpack.redact.RedactProcessor;

import java.util.Map;

/**
 * An external bridge for {@link org.elasticsearch.xpack.redact.RedactPlugin}
 */
public class RedactPluginBridge implements IngestPluginBridge {
    @Override
    public Map<String, ProcessorBridge.Factory> getProcessors(ProcessorBridge.Parameters parameters) {
        // Provide a TRIAL license state to the redact processor
        final XPackLicenseState trialLicenseState = new XPackLicenseState(parameters.toInternal().relativeTimeSupplier);

        return Map.of(
            RedactProcessor.TYPE,
            ProcessorBridge.Factory.fromInternal(new RedactProcessor.Factory(trialLicenseState, parameters.toInternal().matcherWatchdog))
        );
    }
}
