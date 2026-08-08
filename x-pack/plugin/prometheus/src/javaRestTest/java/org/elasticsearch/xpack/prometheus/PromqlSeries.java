/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A series in a Prometheus query response: its labels, without {@code __name__}, and one value. Tests describe the
 * fixture they ingest with the same type, so expectations can be derived from it rather than restated.
 */
record PromqlSeries(Map<String, String> labels, double value) {

    static PromqlSeries of(String label, String labelValue, double value) {
        return new PromqlSeries(Map.of(label, labelValue), value);
    }

    PromqlSeries without(String label) {
        Map<String, String> remaining = new HashMap<>(labels);
        remaining.remove(label);
        return new PromqlSeries(remaining, value);
    }

    PromqlSeries withValue(double newValue) {
        return new PromqlSeries(labels, newValue);
    }

    static List<PromqlSeries> ofInstant(ObjectPath response) throws IOException {
        List<PromqlSeries> series = new ArrayList<>();
        for (int i = 0; i < seriesCount(response); i++) {
            series.add(new PromqlSeries(labels(response, i), value(response.evaluate("data.result." + i + ".value"))));
        }
        return series;
    }

    /** Each series is represented by its last sample. */
    static List<PromqlSeries> ofRange(ObjectPath response) throws IOException {
        List<PromqlSeries> series = new ArrayList<>();
        for (int i = 0; i < seriesCount(response); i++) {
            List<List<Object>> samples = response.evaluate("data.result." + i + ".values");
            series.add(new PromqlSeries(labels(response, i), value(samples.getLast())));
        }
        return series;
    }

    private static int seriesCount(ObjectPath response) throws IOException {
        List<?> result = response.evaluate("data.result");
        return result.size();
    }

    private static Map<String, String> labels(ObjectPath response, int index) throws IOException {
        Map<String, String> labels = new HashMap<>(response.evaluate("data.result." + index + ".metric"));
        labels.remove("__name__");
        return labels;
    }

    /** A sample is the pair {@code [epochSeconds, value]}, with the value rendered as a string. */
    private static double value(List<?> sample) {
        return Double.parseDouble((String) sample.get(1));
    }
}
