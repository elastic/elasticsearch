/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Reduction;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.Accumulator;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.BucketKey;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.SeriesKey;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class DerivedMetricsEmitterTests extends ESTestCase {

    public void testEmittedDocumentCarriesInternalAndUserDimensions() {
        Map<String, Object> document = emit(Reduction.SUM, List.of("service.name", "cloud.region"), List.of("checkout", "eu-west-1"));

        assertEquals("1970-01-01T00:01:00.000Z", document.get("@timestamp"));
        assertEquals("ingest.docs.count", document.get("metric.name"));
        assertEquals("logs-my_app-default", document.get("derived_metrics.source"));
        assertEquals("10s", document.get("derived_metrics.interval"));
        assertEquals("node-1", document.get("derived_metrics.node"));
        assertEquals("checkout", document.get("dimensions.service.name"));
        assertEquals("eu-west-1", document.get("dimensions.cloud.region"));
        assertEquals(6.0, (Double) document.get("metric.value"), 0.0);
    }

    public void testDimensionsAbsentFromTheDocumentAreNotEmitted() {
        Map<String, Object> document = emit(Reduction.SUM, List.of(), List.of());
        assertThat(document, not(hasKey("dimensions.service.name")));
    }

    public void testRateIsDerivedFromTheIntervalLength() {
        Map<String, Object> document = emit(Reduction.RATE, List.of(), List.of());
        // three observations of 1, 2 and 3 over a ten second interval
        assertEquals(0.6, (Double) document.get("metric.value"), 1e-9);
    }

    public void testDestinationIsDerivedFromTheSourceDataStream() {
        IndexRequest request = request(Reduction.SUM, List.of(), List.of());
        assertEquals("derived-metrics-logs-my_app-default", request.index());
        assertEquals(DocWriteRequest.OpType.CREATE, request.opType());
    }

    private static Map<String, Object> emit(Reduction reduction, List<String> dimensionNames, List<String> dimensionValues) {
        IndexRequest request = request(reduction, dimensionNames, dimensionValues);
        return XContentHelper.convertToMap(request.source(), false, request.getContentType()).v2();
    }

    private static IndexRequest request(Reduction reduction, List<String> dimensionNames, List<String> dimensionValues) {
        SeriesKey series = new SeriesKey(
            ProjectId.DEFAULT,
            "logs-my_app-default",
            "ingest.docs.count",
            "10s",
            reduction,
            dimensionNames,
            dimensionValues
        );
        DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(10);
        BucketKey key = new BucketKey(series, 60_000L, 10_000L);
        for (double value : new double[] { 1.0, 2.0, 3.0 }) {
            buffer.record(key, value);
        }
        Accumulator accumulator = buffer.drainAll().get(0).getValue();
        return DerivedMetricsEmitter.toIndexRequest(key, accumulator, "node-1");
    }
}
