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
 * A labelled sample: the labels identifying a series, without {@code __name__}, and a single value.
 *
 * <p>The same type describes the fixture a test ingests and the series a query returned, so expectations can be
 * derived from the fixture with {@link #without} and {@link #withValue} instead of being restated as literals.
 */
record PromqlSeries(Map<String, String> labels, double value) {

    PromqlSeries {
        labels = Map.copyOf(labels);
    }

    /** A series identified by a single label, the usual shape of an aggregation result. */
    static PromqlSeries of(String label, String labelValue, double value) {
        return new PromqlSeries(Map.of(label, labelValue), value);
    }

    /** This series as an aggregation that drops {@code label} should report it. */
    PromqlSeries without(String label) {
        Map<String, String> remaining = new HashMap<>(labels);
        remaining.remove(label);
        return new PromqlSeries(remaining, value);
    }

    /** This series with a different value, keeping its labels. */
    PromqlSeries withValue(double newValue) {
        return new PromqlSeries(labels, newValue);
    }

    /** The series of an instant query response, whose entries carry one {@code value} pair. */
    static List<PromqlSeries> ofInstant(ObjectPath response) throws IOException {
        return parse(response, false);
    }

    /** The series of a range query response, each represented by the last sample of its {@code values} array. */
    static List<PromqlSeries> ofRange(ObjectPath response) throws IOException {
        return parse(response, true);
    }

    private static List<PromqlSeries> parse(ObjectPath response, boolean range) throws IOException {
        List<?> result = response.evaluate("data.result");
        List<PromqlSeries> series = new ArrayList<>(result.size());
        for (int i = 0; i < result.size(); i++) {
            Map<String, String> labels = new HashMap<>(response.evaluate("data.result." + i + ".metric"));
            labels.remove("__name__");
            List<?> sample = range ? lastSample(response, i) : response.evaluate("data.result." + i + ".value");
            // A sample is the pair [epochSeconds, value], with the value rendered as a string.
            series.add(new PromqlSeries(labels, Double.parseDouble((String) sample.get(1))));
        }
        return series;
    }

    private static List<?> lastSample(ObjectPath response, int index) throws IOException {
        List<List<Object>> samples = response.evaluate("data.result." + index + ".values");
        if (samples.isEmpty()) {
            throw new AssertionError("series [" + index + "] carries no samples");
        }
        return samples.getLast();
    }
}
