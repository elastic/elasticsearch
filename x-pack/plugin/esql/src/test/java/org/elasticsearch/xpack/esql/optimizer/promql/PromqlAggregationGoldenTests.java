/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

/** Captures label representation changes at aggregation boundaries, including the physical cost of each translation. */
public class PromqlAggregationGoldenTests extends GoldenTestCase {
    /** Explicit BY labels should remain ordinary columns without loading time-series metadata. */
    public void testDirectBy() {
        aggregate("sum by(cluster, pod)(network.cost)");
    }

    /** Count's null-on-empty expression must stay outside the two aggregation phases. */
    public void testRawCountWithout() {
        aggregate("count without(pod)(network.cost)");
    }

    /** Named regrouping must evaluate the outer count after the inner count's null-on-empty expression. */
    public void testCountWithoutOverCountBy() {
        aggregate("count without(pod)(count by(cluster,pod)(network.cost))");
    }

    /** Open WITHOUT grouping should read one metadata value and exclude only the requested label. */
    public void testRawWithout() {
        aggregate("sum without(pod)(network.cost)");
    }

    /** BY must obtain its labels from the current WITHOUT result, after the inner aggregate completes. */
    public void testByOverWithout() {
        aggregate("sum by(cluster)(sum without(pod)(network.cost))");
    }

    /** WITHOUT over an enumerated label set should regroup existing columns without loading metadata. */
    public void testWithoutOverBy() {
        aggregate("max without(pod)(sum by(cluster, pod)(network.cost))");
    }

    /** Nonlinear nested aggregation must keep both grouping stages and apply exclusions at the proper boundary. */
    public void testNestedNonlinearWithout() {
        aggregate("sum without(cluster)(max without(pod)(network.cost))");
    }

    /** Repeating an exclusion does not remove the second aggregation boundary. */
    public void testRepeatedWithout() {
        aggregate("avg without(pod)(sum without(pod)(network.cost))");
    }

    /** Each level removes its own labels, including a label already removed by its child. */
    public void testOverlappingWithout() {
        aggregate("avg without(cluster, region)(sum without(pod, region)(network.cost))");
    }

    /** Three different functions make the order of grouping boundaries visible in every planning stage. */
    public void testThreeNestedWithout() {
        aggregate("count without(cluster)(max without(region)(sum without(pod)(network.cost)))");
    }

    /** The verifier currently finds nested WITHOUT even across an intervening BY that enumerates all result labels. */
    public void testWithoutOverByOverWithout() {
        aggregate("max without(region)(avg by(cluster, region)(sum without(pod)(network.cost)))");
    }

    /** A global aggregate ends the unknown label set; an outer WITHOUT must not bring source labels back. */
    public void testWithoutOverUngroupedOverWithout() {
        aggregate("count without(cluster)(sum(sum without(pod)(network.cost)))");
    }

    /** Empty exclusions keep the current label set and still evaluate the enclosing aggregate. */
    public void testEmptyWithoutOverWithout() {
        aggregate("max without()(sum without(pod)(network.cost))");
    }

    /** Removing absent labels preserves the identity while both aggregate functions still execute. */
    public void testAbsentWithoutLabels() {
        aggregate("avg without(missing_outer)(sum without(missing_inner)(network.cost))");
    }

    /** Per-series window evaluation must finish before either across-series aggregation changes the label set. */
    public void testNestedWithoutOverRangeAggregation() {
        aggregate("max without(cluster)(sum without(pod)(avg_over_time(network.cost[1h])))");
    }

    /** Empty WITHOUT retains full input identity before the outer BY computes its average. */
    public void testEmptyWithoutUnderBy() {
        aggregate("avg by(cluster)(sum without()(network.cost))");
    }

    private void aggregate(String expression) {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        builder("PROMQL index=k8s step=1h result=(" + expression + ")").since(TransportVersion.current()).run();
    }
}
