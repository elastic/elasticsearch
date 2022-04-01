/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.processor.PluginEntry;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.elasticsearch.logging.impl.provider.Log4JBootstrapSupportImpl;
import org.elasticsearch.logging.spi.ServerSupport;

/**
 * Pattern converter to format the trace id provided in the traceparent header into JSON fields <code>trace.id</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "org.elasticsearch.logging.impl.TraceIdConverter")
@ConverterKeys({ "trace_id" })
public final class TraceIdConverter extends LogEventPatternConverter {
    /**
     * Called by log4j2 to initialize this converter.
     */
    public static TraceIdConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new TraceIdConverter();
    }

    public TraceIdConverter() {
        super("trace_id", "trace_id");
    }

    public static String getTraceId() {
        return ServerSupport.INSTANCE.getTraceIdHeader();
    }

    public static void  init() {
        Log4JBootstrapSupportImpl.initPlugins(PatternConverter.CATEGORY, TraceIdConverter.class, "org.elasticsearch.logging.impl.TraceIdConverter", new PluginEntry());
    }

    /**
     * Formats the trace.id into json fields.
     *
     * @param event - a log event is ignored in this method as it uses the clusterId value
     *              from <code>NodeAndClusterIdStateListener</code> to format
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        String traceId = getTraceId();
        if (traceId != null) {
            toAppendTo.append(traceId);
        }
    }

}
