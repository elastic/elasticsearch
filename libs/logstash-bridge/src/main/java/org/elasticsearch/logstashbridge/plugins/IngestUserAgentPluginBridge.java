/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.plugins;

import org.elasticsearch.ingest.common.UserAgentProcessor;
import org.elasticsearch.logstashbridge.ingest.ProcessorFactoryBridge;
import org.elasticsearch.logstashbridge.ingest.ProcessorParametersBridge;

import java.util.Map;

/**
 * Bridge for the user-agent processor.
 *
 * @deprecated Use {@link IngestCommonPluginBridge} instead, which includes the user_agent processor.
 */
@Deprecated
public final class IngestUserAgentPluginBridge implements IngestPluginBridge {

    @Override
    public Map<String, ProcessorFactoryBridge> getProcessors(final ProcessorParametersBridge parameters) {
        return Map.of(
            UserAgentProcessor.TYPE,
            ProcessorFactoryBridge.fromInternal(
                new UserAgentProcessor.Factory(parameters.toInternal().userAgentParserRegistry)
            )
        );
    }
}
