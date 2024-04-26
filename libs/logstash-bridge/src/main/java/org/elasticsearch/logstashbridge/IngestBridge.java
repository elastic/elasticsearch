/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logstashbridge;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class IngestBridge {
    private IngestBridge() {}

    public static Processor.Parameters newProcessorParameters(
        final Environment environment,
        final ScriptService scriptService,
        final ThreadContext threadContext,
        final LongSupplier relativeTimeSupplier,
        final BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler,
        final Consumer<Runnable> genericExecutor,
        final MatcherWatchdog matcherWatchdog
    ) {
        return new Processor.Parameters(
            environment,
            scriptService,
            null,
            threadContext,
            relativeTimeSupplier,
            scheduler,
            null,
            null,
            genericExecutor,
            matcherWatchdog
        );
    }

    public static IngestDocument newIngestDocument(Map<String, Object> sourceAndMetadata, Map<String, Object> ingestMetadata) {
        return new IngestDocument(sourceAndMetadata, ingestMetadata);
    }

    public ScriptContext<IngestConditionalScript.Factory> ingestConditionalScriptContext() {
        return IngestConditionalScript.CONTEXT;
    }

    public ScriptContext<IngestScript.Factory> ingestScriptContext() {
        return IngestScript.CONTEXT;
    }

    public static MatcherWatchdog createGrokThreadWatchdog(Environment environment, ThreadPool threadPool) {
        return IngestService.createGrokThreadWatchdog(environment, threadPool);
    }
}
