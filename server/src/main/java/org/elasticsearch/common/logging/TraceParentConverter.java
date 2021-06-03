/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.elasticsearch.tasks.Task;

import java.util.Map;

/**
 * Pattern converter to format the cluster_id variable into JSON fields <code>cluster.id</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "TraceParentConverter")
@ConverterKeys({"trace_parent"})
public final class TraceParentConverter extends LogEventPatternConverter {
    /**
     * Called by log4j2 to initialize this converter.
     */
    public static TraceParentConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new TraceParentConverter();
    }

    public TraceParentConverter() {
        super("trace_parent", "trace_parent");
    }

    public static String getTraceParent() {
        return HeaderWarning.THREAD_CONTEXT.stream()
            .filter(t -> t.getHeader(Task.TRACE_PARENT) != null)
            .findFirst()
            .map(t -> {
                Map<String, String> headers = t.getRequestHeadersOnly();
                return headers.get(Task.TRACE_PARENT);

            } )
            .orElse(null);
    }

    /**
     * Formats the cluster.uuid into json fields.
     *
     * @param event - a log event is ignored in this method as it uses the clusterId value
     *              from <code>NodeAndClusterIdStateListener</code> to format
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        String traceParent = getTraceParent();
        if (traceParent != null) {
            toAppendTo.append(traceParent);
        }
    }

}
