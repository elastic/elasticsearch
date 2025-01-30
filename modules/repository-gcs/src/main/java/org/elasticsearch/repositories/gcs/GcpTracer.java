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

import java.util.Collection;

public class GcpTracer extends SpanExporter.Handler {

    public static void createAndRegister() {
        Tracing.getExportComponent().getSpanExporter().registerHandler("gcp-tracer", new GcpTracer());
        TraceConfig traceConfig = Tracing.getTraceConfig();
        traceConfig.updateActiveTraceParams(traceConfig.getActiveTraceParams().toBuilder().setSampler(Samplers.alwaysSample()).build());
    }

    @Override
    public void export(Collection<SpanData> spanDataList) {
        for (var sd : spanDataList) {
            var parent = sd.getParentSpanId();

            System.out.printf(
                "trace=%s parent=%s span=%s name=%s\n",
                sd.getContext().getTraceId().toLowerBase16(),
                parent == null ? "null" : parent.toLowerBase16(),
                sd.getContext().getSpanId().toLowerBase16(),
                sd.getName()
            );
        }

    }
}
