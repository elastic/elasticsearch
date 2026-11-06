/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MetricIdentityTests extends ESTestCase {

    public void testTwoCodecsReportSideBySideUnderTheirOwnGroups() {
        final ProfileCollector stats = TestCollectors.countersOnly();
        stats.counter(CounterMetric.of("columnar", "test", "widgets_read", Metric.Unit.COUNT, "widgets the read fetched")).add(3);
        stats.counter(CounterMetric.of("faux", "test", "widgets_read", Metric.Unit.COUNT, "widgets the other codec fetched")).add(5);
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("columnar.test.widgets_read"), equalTo(3L));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("faux.test.widgets_read"), equalTo(5L));
    }

    public void testSameNameInAnotherGroupIsADifferentMetric() {
        final ProfileCollector stats = TestCollectors.countersOnly();
        final Counter first = stats.counter(CounterMetric.of("one", "test", "events", Metric.Unit.COUNT, "events of one"));
        final Counter second = stats.counter(CounterMetric.of("two", "test", "events", Metric.Unit.COUNT, "events of two"));
        first.add(1);
        second.add(2);
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("one.test.events"), equalTo(1L));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("two.test.events"), equalTo(2L));
    }

    public void testAQualifiedNameKeepsOneDefinition() {
        final ProfileCollector stats = TestCollectors.countersOnly();
        stats.counter(CounterMetric.of("g", "test", "payload_bytes", Metric.Unit.BYTES, "stored payload size"));
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> stats.counter(CounterMetric.of("g", "test", "payload_bytes", Metric.Unit.COUNT, "payloads fetched"))
        );
        assertThat(e.getMessage(), containsString("g.test.payload_bytes"));
        assertThat(e.getMessage(), containsString("already registered"));
    }

    public void testAGroupAllowsNoSeparatorAmbiguity() {
        expectThrows(
            IllegalArgumentException.class,
            () -> CounterMetric.of("Bad.Group", "test", "events", Metric.Unit.COUNT, "events of a malformed group")
        );
        expectThrows(IllegalArgumentException.class, () -> CounterMetric.of("", "test", "events", Metric.Unit.COUNT, "empty group"));
    }

    public void testAGroupKeepsTheCaseOfTheCodecName() {
        final CounterMetric metric = CounterMetric.of("ColumNAR", "blocks", "read", Metric.Unit.COUNT, "blocks fetched");
        assertThat(metric.id().key(), equalTo("ColumNAR.blocks.read"));
        for (String group : List.of("1codec", "_codec", "Cod\u00e9", "co-dec", "co dec")) {
            expectThrows(
                IllegalArgumentException.class,
                () -> CounterMetric.of(group, "blocks", "read", Metric.Unit.COUNT, "a group that is not an identifier")
            );
        }
    }

    public void testAComponentAndANameStayLowerCase() {
        expectThrows(
            IllegalArgumentException.class,
            () -> CounterMetric.of("ColumNAR", "Blocks", "read", Metric.Unit.COUNT, "an upper case component")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> CounterMetric.of("ColumNAR", "blocks", "Read", Metric.Unit.COUNT, "an upper case name")
        );
    }

    public void testAComponentAllowsNoSeparatorAmbiguity() {
        expectThrows(
            IllegalArgumentException.class,
            () -> CounterMetric.of("columnar", "Bad.Component", "events", Metric.Unit.COUNT, "events of a malformed component")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> CounterMetric.of("columnar", "", "events", Metric.Unit.COUNT, "empty component")
        );
    }

    public void testTheQualifiedNameJoinsGroupComponentAndName() {
        assertThat(
            CounterMetric.of("columnar", "blocks", "read", Metric.Unit.COUNT, "blocks fetched").id().key(),
            equalTo("columnar.blocks.read")
        );
    }
}
