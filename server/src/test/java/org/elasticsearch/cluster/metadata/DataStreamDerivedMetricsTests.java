/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.GaugeAggregation;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.Metric;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.MetricType;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.MetricValue;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamDerivedMetricsTests extends AbstractXContentSerializingTestCase<DataStreamDerivedMetrics> {

    @Override
    protected Writeable.Reader<DataStreamDerivedMetrics> instanceReader() {
        return DataStreamDerivedMetrics::read;
    }

    @Override
    protected DataStreamDerivedMetrics createTestInstance() {
        return randomDerivedMetrics();
    }

    public static DataStreamDerivedMetrics randomDerivedMetrics() {
        return new DataStreamDerivedMetrics(
            randomBoolean(),
            randomSubsetOf(randomIntBetween(1, 3), "ingest.*", "ingest.docs.count", "ingest.failures.rate"),
            randomList(1, 3, () -> TimeValue.timeValueSeconds(randomIntBetween(1, 3600))),
            randomList(0, 3, DataStreamDerivedMetricsTests::randomFieldName),
            randomList(0, 3, DataStreamDerivedMetricsTests::randomMetric)
        );
    }

    public static DataStreamDerivedMetrics.Template randomTemplate() {
        return new DataStreamDerivedMetrics.Template(
            randomBoolean() ? randomBoolean() : null,
            randomBoolean() ? randomSubsetOf(randomIntBetween(1, 3), "ingest.*", "ingest.docs.rate", "ingest.bytes.rate") : null,
            randomBoolean() ? randomList(1, 3, () -> TimeValue.timeValueSeconds(randomIntBetween(1, 3600))) : null,
            randomBoolean() ? randomList(0, 3, DataStreamDerivedMetricsTests::randomFieldName) : null,
            randomBoolean() ? randomList(0, 3, DataStreamDerivedMetricsTests::randomMetric) : null
        );
    }

    private static Metric randomMetric() {
        MetricType type = randomFrom(MetricType.values());
        return switch (type) {
            case COUNTER -> new Metric(
                "metric." + randomAlphaOfLength(8),
                type,
                randomPredicate(),
                randomBoolean() ? MetricValue.constant(randomDoubleBetween(0.0, 1000.0, true)) : MetricValue.field(randomFieldName()),
                null,
                randomList(0, 2, DataStreamDerivedMetricsTests::randomFieldName)
            );
            case GAUGE -> new Metric(
                "metric." + randomAlphaOfLength(8),
                type,
                randomPredicate(),
                MetricValue.field(randomFieldName()),
                randomFrom(GaugeAggregation.values()),
                randomList(0, 2, DataStreamDerivedMetricsTests::randomFieldName)
            );
            case HISTOGRAM -> new Metric(
                "metric." + randomAlphaOfLength(8),
                type,
                randomPredicate(),
                MetricValue.field(randomFieldName()),
                null,
                randomList(0, 2, DataStreamDerivedMetricsTests::randomFieldName)
            );
        };
    }

    private static Map<String, Object> randomPredicate() {
        return randomBoolean() ? null : Map.of("exists", Map.of("field", randomFieldName()));
    }

    private static String randomFieldName() {
        return randomAlphaOfLength(5) + "." + randomAlphaOfLength(5);
    }

    @Override
    protected DataStreamDerivedMetrics mutateInstance(DataStreamDerivedMetrics instance) throws IOException {
        return switch (randomIntBetween(0, 4)) {
            case 0 -> new DataStreamDerivedMetrics(
                instance.enabled() == false,
                instance.builtin(),
                instance.intervals(),
                instance.dimensions(),
                instance.metrics()
            );
            case 1 -> new DataStreamDerivedMetrics(
                instance.enabled(),
                List.of("ingest.bytes.rate"),
                instance.intervals(),
                instance.dimensions(),
                instance.metrics()
            );
            case 2 -> new DataStreamDerivedMetrics(
                instance.enabled(),
                instance.builtin(),
                List.of(TimeValue.timeValueMinutes(5)),
                instance.dimensions(),
                instance.metrics()
            );
            case 3 -> new DataStreamDerivedMetrics(
                instance.enabled(),
                instance.builtin(),
                instance.intervals(),
                List.of("service.name"),
                instance.metrics()
            );
            case 4 -> new DataStreamDerivedMetrics(
                instance.enabled(),
                instance.builtin(),
                instance.intervals(),
                instance.dimensions(),
                List.of(new Metric("metric.mutated", MetricType.COUNTER, null, MetricValue.constant(1), null, List.of()))
            );
            default -> throw new IllegalArgumentException("Illegal randomisation branch");
        };
    }

    @Override
    protected DataStreamDerivedMetrics doParseInstance(XContentParser parser) throws IOException {
        return DataStreamDerivedMetrics.fromXContent(parser);
    }

    public void testDefaults() {
        DataStreamDerivedMetrics metrics = DataStreamDerivedMetrics.fromTemplate(
            new DataStreamDerivedMetrics.Template(null, null, null, null, null)
        );
        assertThat(metrics.enabled(), equalTo(true));
        assertThat(metrics.builtin(), equalTo(List.of("ingest.*")));
        assertThat(metrics.intervals(), equalTo(List.of(TimeValue.timeValueSeconds(10))));
        assertThat(metrics.dimensions(), equalTo(List.of()));
        assertThat(metrics.metrics(), equalTo(List.of()));
    }

    public void testGaugeAggregationModes() {
        for (GaugeAggregation aggregation : GaugeAggregation.values()) {
            Metric metric = new Metric("app.queue.depth", MetricType.GAUGE, null, MetricValue.field("queue.depth"), aggregation, List.of());
            assertThat(metric.aggregation(), equalTo(aggregation));
        }
        Metric defaulted = new Metric("app.queue.depth", MetricType.GAUGE, null, MetricValue.field("queue.depth"), null, List.of());
        assertThat(defaulted.aggregation(), equalTo(GaugeAggregation.LAST_VALUE));
    }

    public void testCounterDefaultsValueToOne() {
        Metric metric = new Metric("app.events", MetricType.COUNTER, null, null, null, List.of());
        assertThat(metric.value(), equalTo(MetricValue.constant(1.0)));
    }

    public void testPredicateOperators() {
        List<Map<String, Object>> predicates = List.of(
            Map.of("exists", Map.of("field", "event.duration")),
            Map.of("term", Map.of("event.outcome", "failure")),
            Map.of("terms", Map.of("http.response.status_code", List.of(500, 502))),
            Map.of("range", Map.of("http.response.status_code", Map.of("gte", 500))),
            Map.of(
                "and",
                List.of(Map.of("exists", Map.of("field", "event.duration")), Map.of("range", Map.of("event.duration", Map.of("gt", 0))))
            ),
            Map.of("or", List.of(Map.of("term", Map.of("log.level", "error")), Map.of("term", Map.of("event.outcome", "failure")))),
            Map.of("not", Map.of("term", Map.of("event.outcome", "success")))
        );
        for (Map<String, Object> predicate : predicates) {
            Metric metric = new Metric("app.metric." + predicates.indexOf(predicate), MetricType.COUNTER, predicate, null, null, List.of());
            assertThat(metric.when(), equalTo(predicate));
        }
    }

    public void testRejectsReservedUserMetricNames() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new Metric("ingest.docs.count", MetricType.COUNTER, null, null, null, List.of())
        );
        assertThat(e.getMessage(), containsString("uses reserved [ingest.*] namespace"));
    }

    public void testRejectsAggregationOnNonGauge() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new Metric("app.events", MetricType.COUNTER, null, MetricValue.constant(1), GaugeAggregation.SUM, List.of())
        );
        assertThat(e.getMessage(), containsString("only supports [aggregation] for gauge metrics"));
    }

    public void testRejectsScriptsInMetricValue() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "name": "app.latency",
              "type": "gauge",
              "value": { "script": "now() - doc['@timestamp']" }
            }
            """);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Metric.fromXContent(parser));
        assertThat(e.getMessage(), containsString("failed to parse field [value]"));
        assertThat(e.getCause().getMessage(), containsString("value object must contain only [field]"));
    }

    public void testTemplateCompositionIsAdditive() {
        Metric requests = new Metric("http.requests", MetricType.COUNTER, null, null, null, List.of("http.request.method"));
        Metric errors = new Metric(
            "http.errors",
            MetricType.COUNTER,
            Map.of("range", Map.of("http.response.status_code", Map.of("gte", 500))),
            null,
            null,
            List.of()
        );
        DataStreamDerivedMetrics.Template base = new DataStreamDerivedMetrics.Template(
            true,
            List.of("ingest.docs.rate"),
            List.of(TimeValue.timeValueSeconds(10)),
            List.of("service.name"),
            List.of(requests)
        );
        DataStreamDerivedMetrics.Template extra = new DataStreamDerivedMetrics.Template(
            null,
            List.of("ingest.failures.rate"),
            List.of(TimeValue.timeValueMinutes(1)),
            List.of("host.name"),
            List.of(errors)
        );

        DataStreamDerivedMetrics result = new DataStreamDerivedMetrics.Builder(base).composeTemplate(extra).build();
        assertThat(result.builtin(), equalTo(List.of("ingest.docs.rate", "ingest.failures.rate")));
        assertThat(result.intervals(), equalTo(List.of(TimeValue.timeValueSeconds(10), TimeValue.timeValueMinutes(1))));
        assertThat(result.dimensions(), equalTo(List.of("service.name", "host.name")));
        assertThat(result.metrics(), equalTo(List.of(requests, errors)));
    }

    public void testTemplateCompositionRejectsConflictingMetricNames() {
        Metric count = new Metric("http.requests", MetricType.COUNTER, null, null, null, List.of());
        Metric duration = new Metric("http.requests", MetricType.HISTOGRAM, null, MetricValue.field("event.duration"), null, List.of());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DataStreamDerivedMetrics.Builder(new DataStreamDerivedMetrics.Template(null, null, null, null, List.of(count)))
                .composeTemplate(new DataStreamDerivedMetrics.Template(null, null, null, null, List.of(duration)))
        );
        assertThat(e.getMessage(), containsString("is defined more than once"));
    }

    public void testTemplateKeepsOmittedFieldsUndefined() {
        DataStreamDerivedMetrics.Template template = new DataStreamDerivedMetrics.Template(null, List.of("ingest.*"), null, null, null);
        assertThat(template.enabled(), nullValue());
        assertThat(template.builtin(), equalTo(List.of("ingest.*")));
        assertThat(template.intervals(), nullValue());
    }
}
