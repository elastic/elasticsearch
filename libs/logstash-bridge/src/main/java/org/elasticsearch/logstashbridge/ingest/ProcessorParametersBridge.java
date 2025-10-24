/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.env.EnvironmentBridge;
import org.elasticsearch.logstashbridge.script.ScriptServiceBridge;
import org.elasticsearch.logstashbridge.threadpool.ThreadPoolBridge;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

public interface ProcessorParametersBridge extends StableBridgeAPI<Processor.Parameters> {

    static ProcessorParametersBridge create(
        final EnvironmentBridge bridgedEnvironment,
        final ScriptServiceBridge bridgedScriptService,
        final ThreadPoolBridge bridgedThreadPool
    ) {
        Environment environment = bridgedEnvironment.toInternal();
        ScriptService scriptService = bridgedScriptService.toInternal();
        ThreadPool threadPool = bridgedThreadPool.toInternal();

        final Processor.Parameters processorParameters = new Processor.Parameters(
            environment,
            scriptService,
            null,
            threadPool.getThreadContext(),
            threadPool::relativeTimeInMillis,
            (delay, command) -> threadPool.schedule(command, TimeValue.timeValueMillis(delay), threadPool.generic()),
            null,
            null,
            threadPool.generic()::execute,
            IngestService.createGrokThreadWatchdog(environment, threadPool)
        );
        return new ProxyInternal(processorParameters);
    }

    /**
     * An implementation of {@link ProcessorParametersBridge} that proxies calls through
     * to an internal {@link Processor.Parameters}.
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<Processor.Parameters> implements ProcessorParametersBridge {

        private ProxyInternal(final Processor.Parameters delegate) {
            super(delegate);
        }

        @Override
        public Processor.Parameters toInternal() {
            return this.internalDelegate;
        }
    }
}
