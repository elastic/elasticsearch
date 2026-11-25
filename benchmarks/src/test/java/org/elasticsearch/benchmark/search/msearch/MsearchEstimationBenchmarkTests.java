/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.msearch;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Smoke-tests {@link MsearchEstimationBenchmark}: verifies that every {@code responseShape} sets up
 * without throwing, that {@link TransportMultiSearchAction#estimateActualBytes} returns a positive
 * value for each shape, and that shapes with additional components produce larger estimates than
 * simpler shapes (catching silent no-ops in optional branches). Runs with a fixed {@code hitCount=10}
 * to keep CI time small.
 */
public class MsearchEstimationBenchmarkTests extends ESTestCase {

    private final String responseShape;

    public MsearchEstimationBenchmarkTests(String responseShape) {
        this.responseShape = responseShape;
    }

    public void testSetupAndEstimate() {
        var bench = new MsearchEstimationBenchmark();
        bench.hitCount = 10;
        bench.responseShape = responseShape;
        bench.setup();
        try {
            long estimate = TransportMultiSearchAction.estimateActualBytes(bench.response);
            assertThat("responseShape=" + responseShape + " should produce a positive estimate", estimate, greaterThan(0L));
        } finally {
            bench.tearDown();
        }
    }

    /** Shapes with additional per-hit components must produce strictly larger estimates than {@code base}. */
    public void testRicherShapesExceedBase() {
        long base = estimate("base");
        for (String richer : new String[] { "highlights", "explanations", "sortValues", "innerHits" }) {
            assertThat(richer + " estimate should exceed base (" + base + ")", estimate(richer), greaterThan(base));
        }
        assertThat("deepInnerHits estimate should exceed innerHits", estimate("deepInnerHits"), greaterThan(estimate("innerHits")));
        assertThat("full estimate should exceed base (" + base + ")", estimate("full"), greaterThan(base));
    }

    private static long estimate(String shape) {
        var bench = new MsearchEstimationBenchmark();
        bench.hitCount = 10;
        bench.responseShape = shape;
        bench.setup();
        try {
            return TransportMultiSearchAction.estimateActualBytes(bench.response);
        } finally {
            bench.tearDown();
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        String[] shapes = Utils.possibleValues(MsearchEstimationBenchmark.class, "responseShape").toArray(new String[0]);
        return () -> Arrays.stream(shapes).map(s -> new Object[] { s }).iterator();
    }
}
