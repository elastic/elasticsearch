/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.Metric;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.MetricType;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.MetricValue;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * The rule under test is asymmetric on purpose, and both halves matter: a field the mapping gives a type that cannot serve the
 * configuration is rejected, because no future document can rescue it; a field the mapping does not mention is accepted, because that is
 * what configuring a metric before its data looks like.
 */
public class DerivedMetricsMappingValidatorTests extends ESTestCase {

    private static final String MAPPING = """
        {
          "_doc": {
            "properties": {
              "@timestamp": { "type": "date" },
              "service": { "properties": { "name": { "type": "keyword" } } },
              "event": { "properties": { "duration": { "type": "long" } } },
              "http": { "properties": { "response": { "properties": { "status_code": { "type": "long" } } } } },
              "labels": { "type": "object" },
              "attributes.region": { "type": "keyword" }
            }
          }
        }""";

    /**
     * A metric whose value points at a keyword yields no number from any document, so it would emit nothing for as long as it stayed
     * configured. That is exactly the misconfiguration worth catching, because nothing about it looks broken from the outside.
     */
    public void testAValueFieldThatCannotBeANumberIsRejected() throws IOException {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validate(config(gauge("queue.depth", MetricValue.field("service.name"))))
        );
        assertThat(e.getMessage(), containsString("[service.name]"));
        assertThat(e.getMessage(), containsString("mapped as [keyword]"));
        assertThat(e.getMessage(), containsString("must be numeric"));
    }

    public void testANumericValueFieldIsAccepted() throws IOException {
        validate(config(gauge("latency", MetricValue.field("event.duration"))));
        // nested more than one level, to prove the path walk does not stop at the first object
        validate(config(gauge("status", MetricValue.field("http.response.status_code"))));
    }

    /**
     * A field written with dots in a single mapping key is as legal as one nested object by object, and templates use both.
     */
    public void testADottedMappingKeyResolvesToo() throws IOException {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validate(config(gauge("region.depth", MetricValue.field("attributes.region"))))
        );
        assertThat(e.getMessage(), containsString("mapped as [keyword]"));
    }

    /** An object holds other fields rather than a value, so there is nothing for a dimension to be. */
    public void testADimensionOnAnObjectIsRejected() throws IOException {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validate(new DataStreamDerivedMetrics(true, List.of("ingest.docs.count"), null, null, List.of("labels"), List.of()))
        );
        assertThat(e.getMessage(), containsString("[labels]"));
        assertThat(e.getMessage(), containsString("holds other fields"));
    }

    /** A range needs an ordering, and a keyword has none that the predicate could use. */
    public void testARangePredicateOnAKeywordIsRejected() throws IOException {
        Metric metric = new Metric(
            "http.slow",
            MetricType.COUNTER,
            Map.of("range", Map.of("service.name", Map.of("gte", 100))),
            null,
            null,
            null,
            null
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validate(config(metric)));
        assertThat(e.getMessage(), containsString("range predicate"));
        assertThat(e.getMessage(), containsString("[service.name]"));
    }

    /** The same check has to reach inside the boolean operators, or it is trivial to hide a bad field one level down. */
    public void testARangeNestedInsideAnOperatorIsStillChecked() throws IOException {
        Metric metric = new Metric(
            "http.slow",
            MetricType.COUNTER,
            Map.of(
                "and",
                List.of(Map.of("exists", Map.of("field", "service.name")), Map.of("range", Map.of("service.name", Map.of("gte", 100))))
            ),
            null,
            null,
            null,
            null
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validate(config(metric)));
        assertThat(e.getMessage(), containsString("range predicate"));
    }

    /** term and terms work against whatever the field turns out to be, so a mapping cannot contradict them. */
    public void testTermPredicatesAreNotSecondGuessed() throws IOException {
        validate(config(new Metric("a", MetricType.COUNTER, Map.of("term", Map.of("service.name", "checkout")), null, null, null, null)));
        validate(config(new Metric("b", MetricType.COUNTER, Map.of("exists", Map.of("field", "labels")), null, null, null, null)));
    }

    /**
     * The half that must not become an error. A metric is very often configured before the field it names has ever been written, and a
     * dynamically mapped stream only gains the field when a document brings it — so an absence is a promise of data, not a mistake.
     */
    public void testAFieldTheMappingHasNeverSeenIsAccepted() throws IOException {
        validate(config(gauge("queue.depth", MetricValue.field("nothing.maps.this"))));
        validate(new DataStreamDerivedMetrics(true, List.of(), null, null, List.of("also.unmapped"), List.of()));
    }

    /** With no index yet there is nothing to check against, which is the state a stream is in when it is first configured. */
    public void testNothingIsRejectedBeforeThereIsAMapping() {
        DerivedMetricsMappingValidator.validate("logs-my_app-default", config(gauge("q", MetricValue.field("service.name"))), null);
    }

    private static void validate(DataStreamDerivedMetrics config) throws IOException {
        DerivedMetricsMappingValidator.validate("logs-my_app-default", config, new MappingMetadata(new CompressedXContent(MAPPING)));
    }

    private static Metric gauge(String name, MetricValue value) {
        return new Metric(name, MetricType.GAUGE, null, value, null, null, null);
    }

    private static DataStreamDerivedMetrics config(Metric metric) {
        return new DataStreamDerivedMetrics(true, List.of(), null, null, List.of(), List.of(metric));
    }
}
