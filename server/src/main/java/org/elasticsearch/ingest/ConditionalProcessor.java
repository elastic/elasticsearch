/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;

import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * A wrapping processor that adds 'if' logic around the wrapped processor.
 */
public class ConditionalProcessor extends AbstractProcessor implements WrappingProcessor {

    static final String TYPE = "conditional";

    private final Script condition;
    private final ScriptService scriptService;
    private final Processor processor;
    private final IngestMetric metric;
    private final LongSupplier relativeTimeProvider;
    private final IngestConditionalScript.Factory precompiledConditionalScriptFactory;

    ConditionalProcessor(String tag, String description, Script script, ScriptService scriptService, Processor processor) {
        this(tag, description, script, scriptService, processor, System::nanoTime);
    }

    ConditionalProcessor(
        String tag,
        String description,
        Script script,
        ScriptService scriptService,
        Processor processor,
        LongSupplier relativeTimeProvider
    ) {
        super(tag, description);
        this.condition = script;
        this.scriptService = scriptService;
        this.processor = processor;
        this.metric = new IngestMetric();
        this.relativeTimeProvider = relativeTimeProvider;

        try {
            if (ScriptType.INLINE.equals(script.getType())) {
                precompiledConditionalScriptFactory = scriptService.compile(script, IngestConditionalScript.CONTEXT);
            } else {
                // stored script, so will have to compile at runtime
                precompiledConditionalScriptFactory = null;
            }
        } catch (ScriptException e) {
            throw newConfigurationException(TYPE, tag, null, e);
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        assert isAsync() == false;

        final boolean matches = evaluate(ingestDocument);
        if (matches) {
            long startTimeInNanos = relativeTimeProvider.getAsLong();
            try {
                metric.preIngest();
                return processor.execute(ingestDocument);
            } catch (Exception e) {
                metric.ingestFailed();
                throw e;
            } finally {
                long ingestTimeInNanos = relativeTimeProvider.getAsLong() - startTimeInNanos;
                metric.postIngest(ingestTimeInNanos);
            }
        }
        return ingestDocument;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        assert isAsync();
        final boolean matches;
        try {
            matches = evaluate(ingestDocument);
        } catch (Exception e) {
            handler.accept(null, e);
            return;
        }

        if (matches) {
            final long startTimeInNanos = relativeTimeProvider.getAsLong();
            metric.preIngest();
            processor.execute(ingestDocument, (result, e) -> {
                long ingestTimeInNanos = relativeTimeProvider.getAsLong() - startTimeInNanos;
                metric.postIngest(ingestTimeInNanos);
                if (e != null) {
                    metric.ingestFailed();
                    handler.accept(null, e);
                } else {
                    handler.accept(result, null);
                }
            });
        } else {
            handler.accept(ingestDocument, null);
        }
    }

    boolean evaluate(IngestDocument ingestDocument) {
        IngestConditionalScript.Factory factory = precompiledConditionalScriptFactory;
        if (factory == null) {
            factory = scriptService.compile(condition, IngestConditionalScript.CONTEXT);
        }
        return factory.newInstance(condition.getParams(), ingestDocument.getUnmodifiableSourceAndMetadata()).execute();
    }

    public Processor getInnerProcessor() {
        return processor;
    }

    IngestMetric getMetric() {
        return metric;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public String getCondition() {
        return condition.getIdOrCode();
    }
}
