/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.message.BasicNameValuePair;
import org.elasticsearch.client.Request;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Executable regressions for expression composition found by pentest2. Inputs change between steps so
 * assertions must retain every timestamp, complete label identity and result type, not just the last sample.
 */
public class PromqlCompositionRestIT extends AbstractPrometheusRestIT {
    private static final Instant START = Instant.parse("2026-01-01T00:02:00Z");
    private static final Map<String, String> A = Map.of("host", "a", "cluster", "prod");
    private static final Map<String, String> B = Map.of("host", "b", "cluster", "prod");
    private static final Map<String, String> C = Map.of("host", "c", "cluster", "qa");

    /** A literal vector uses the aggregate's row frame in either operand position. */
    public void testLiteralVectorWithAggregate() throws Exception {
        ingestCompositionData();
        var plus = List.of(List.of(new Point(Map.of(), 53)), List.of(new Point(Map.of(), 58)), List.of(new Point(Map.of(), 83)));
        assertAllSteps("vector(1) + sum(tx)", plus);
        assertAllSteps("sum(tx) + vector(1)", plus);
        assertAllSteps(
            "vector(1) - sum(tx)",
            List.of(List.of(new Point(Map.of(), -51)), List.of(new Point(Map.of(), -56)), List.of(new Point(Map.of(), -81)))
        );
        assertAllSteps(
            "sum(tx) - vector(1)",
            List.of(List.of(new Point(Map.of(), 51)), List.of(new Point(Map.of(), 56)), List.of(new Point(Map.of(), 81)))
        );
    }

    private record Point(Map<String, String> labels, double value) {}

    private void ingestCompositionData() throws IOException {
        var request = RemoteWrite.WriteRequest.newBuilder();
        addSeries(request, "tx", A, START, 10, 40, 20);
        addSeries(request, "tx", B, START, 30, 5, 50);
        addSeries(request, "tx", C, START, 12, 12, 12);
        ingestTestData(request.build());
    }

    private static void addSeries(
        RemoteWrite.WriteRequest.Builder request,
        String metric,
        Map<String, String> dimensions,
        Instant start,
        double... values
    ) {
        var series = RemoteWrite.TimeSeries.newBuilder().addLabels(label("__name__", metric));
        dimensions.forEach((key, value) -> series.addLabels(label(key, value)));
        for (int step = 0; step < values.length; step++) {
            series.addSamples(sample(values[step], start.plusSeconds(step * 60L).toEpochMilli()));
        }
        request.addTimeseries(series);
    }

    private void assertAllSteps(String query, List<List<Point>> steps) throws IOException {
        assertEquals("fixture has three evaluation steps", 3, steps.size());
        for (int step = 0; step < steps.size(); step++) {
            Request request = prometheusReadRequest(
                "/_prometheus/api/v1/query",
                new BasicNameValuePair("query", query),
                new BasicNameValuePair("time", START.plusSeconds(step * 60L).toString())
            );
            assertResult(
                query,
                ObjectPath.createFromResponse(client().performRequest(request)),
                false,
                expected(steps.subList(step, step + 1), step)
            );
        }
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query_range",
            new BasicNameValuePair("query", query),
            new BasicNameValuePair("start", START.toString()),
            new BasicNameValuePair("end", START.plusSeconds(120).toString()),
            new BasicNameValuePair("step", "60s")
        );
        assertResult(query, ObjectPath.createFromResponse(client().performRequest(request)), true, expected(steps, 0));
    }

    private static Map<Map<String, String>, Map<Double, Double>> expected(List<List<Point>> steps, int firstStep) {
        var result = new HashMap<Map<String, String>, Map<Double, Double>>();
        for (int step = 0; step < steps.size(); step++) {
            double timestamp = START.plusSeconds((firstStep + step) * 60L).getEpochSecond();
            for (Point point : steps.get(step)) {
                assertNull(
                    "duplicate expected identity",
                    result.computeIfAbsent(point.labels(), ignored -> new HashMap<>()).put(timestamp, point.value())
                );
            }
        }
        return result;
    }

    private static void assertResult(
        String query,
        ObjectPath response,
        boolean range,
        Map<Map<String, String>, Map<Double, Double>> expected
    ) throws IOException {
        assertThat(query, response.evaluate("status"), equalTo("success"));
        assertThat(query, response.evaluate("data.resultType"), equalTo(range ? "matrix" : "vector"));
        List<Map<String, Object>> series = response.evaluate("data.result");
        var actual = new HashMap<Map<String, String>, Map<Double, Double>>();
        for (Map<String, Object> item : series) {
            var path = new ObjectPath(item);
            Map<String, String> labels = path.evaluate("metric");
            var points = new HashMap<Double, Double>();
            assertNull(query + ": duplicate output identity " + labels, actual.put(labels, points));
            List<List<Object>> samples;
            if (range) {
                samples = path.evaluate("values");
            } else {
                List<Object> value = path.evaluate("value");
                samples = List.of(value);
            }
            assertFalse(query + ": empty output series " + labels, samples.isEmpty());
            for (List<Object> sample : samples) {
                assertEquals(query + ": sample must have a timestamp and value", 2, sample.size());
                double timestamp = ((Number) sample.get(0)).doubleValue();
                double value = Double.parseDouble(sample.get(1).toString());
                assertNull(query + ": duplicate output timestamp " + timestamp, points.put(timestamp, value));
            }
        }
        assertEquals(query + ": complete output identities", expected.keySet(), actual.keySet());
        expected.forEach((labels, points) -> {
            assertEquals(query + ": timestamps for " + labels, points.keySet(), actual.get(labels).keySet());
            points.forEach(
                (timestamp, value) -> assertThat(
                    query + " " + labels + " at " + timestamp,
                    actual.get(labels).get(timestamp),
                    closeTo(value, 1e-10)
                )
            );
        });
    }
}
