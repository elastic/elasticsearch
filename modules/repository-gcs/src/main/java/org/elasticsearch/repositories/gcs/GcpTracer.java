/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanExporter;
import io.opencensus.trace.samplers.Samplers;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;

public class GcpTracer extends SpanExporter.Handler {

    private static final Logger logger = LogManager.getLogger(GcpTracer.class);

    public static void createAndRegister() {
        Tracing.getExportComponent().getSpanExporter().registerHandler("gcp-tracer", new GcpTracer());
        TraceConfig traceConfig = Tracing.getTraceConfig();
        traceConfig.updateActiveTraceParams(traceConfig.getActiveTraceParams().toBuilder().setSampler(Samplers.alwaysSample()).build());
    }

    public static void shutdown() {
        Tracing.getExportComponent().shutdown();
    }

    static String parentSpanId(SpanData sd) {
        var ps = sd.getParentSpanId();
        return ps == null ? ".null" : ps.toLowerBase16();
    }

    static String traceId(SpanData sd) {
        return sd.getContext().getTraceId().toLowerBase16();
    }

    @Override
    public void export(Collection<SpanData> spanDataList) {
        var out = new ArrayList<>(spanDataList);
        out.sort(Comparator.comparing(GcpTracer::traceId).thenComparing(GcpTracer::parentSpanId));
        var sb = new StringBuilder();
        var f = "%32s\t%16s\t%16s\t%s\n";
        sb.append('\n');
        sb.append(String.format(f, "trace", "parent", "span", "name"));
        for (var sd : out) {
            sb.append(String.format(f, traceId(sd), parentSpanId(sd), sd.getContext().getSpanId().toLowerBase16(), sd.getName()));
        }
        logger.info(sb.toString());
    }
}
