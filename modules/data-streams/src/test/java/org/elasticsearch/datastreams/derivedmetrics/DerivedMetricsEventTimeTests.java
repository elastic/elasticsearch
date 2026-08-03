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
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.TimeSource;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDocumentReader.Strategies;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

/**
 * A metric is bucketed by one of two clocks, and which one it is changes what its numbers mean. The built-in ingest metrics count writes,
 * so they belong to the moment of the write. A user metric counts something that happened before the write, so it belongs to the moment
 * the document says it happened; a pipeline running behind would otherwise shift the whole series by however far behind it is.
 */
public class DerivedMetricsEventTimeTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        // brings the _data_stream_timestamp field mapper, without which the timestamp is never stashed for reuse
        return List.of(new DataStreamsPlugin(Settings.EMPTY));
    }

    // The timestamp is only stashed in the keyed slot this reads when the data stream timestamp field is enabled, which is what makes it
    // cheap for a data stream and absent for anything else. Derived metrics only ever observes data streams, so that is the mapping to
    // test against.
    private static final String MILLIS_MAPPING = """
        { "_doc": {
            "_data_stream_timestamp": { "enabled": true },
            "properties": {
              "@timestamp": { "type": "date" },
              "service.name": { "type": "keyword" },
              "event.duration": { "type": "long" }
            } } }""";

    private static final String NANOS_MAPPING = """
        { "_doc": {
            "_data_stream_timestamp": { "enabled": true },
            "properties": {
              "@timestamp": { "type": "date_nanos" },
              "service.name": { "type": "keyword" }
            } } }""";

    public void testAUserMetricDefaultsToTheDocumentsOwnTimestamp() {
        Metric metric = new Metric("http.requests", MetricType.COUNTER, null, null, null, null, null);
        assertEquals(TimeSource.EVENT, metric.timeSourceOrDefault());

        CompiledDerivedMetrics compiled = CompiledDerivedMetrics.compile(
            new DataStreamDerivedMetrics(true, List.of(), null, null, List.of(), List.of(metric))
        );
        assertEquals(TimeSource.EVENT, compiled.metrics().get(0).timeSource());
    }

    /**
     * The built-ins measure the act of ingesting, so the clock that matters is the one at the moment of the write. This is not
     * configurable, because a document's own timestamp says nothing about when it was ingested.
     */
    public void testBuiltInMetricsAreAlwaysBucketedByTheWriteClock() {
        CompiledDerivedMetrics compiled = CompiledDerivedMetrics.compile(
            new DataStreamDerivedMetrics(true, List.of("ingest.*"), null, null, List.of(), List.of())
        );
        assertFalse(compiled.metrics().isEmpty());
        for (CompiledDerivedMetrics.CompiledMetric metric : compiled.metrics()) {
            assertEquals("built-in [" + metric.name() + "] must count writes", TimeSource.INGEST, metric.timeSource());
        }
    }

    public void testAUserMetricCanAskForTheWriteClockInstead() {
        Metric metric = new Metric(
            "queue.depth",
            MetricType.GAUGE,
            null,
            MetricValue.field("event.duration"),
            DataStreamDerivedMetrics.GaugeAggregation.MAX,
            null,
            null,
            null,
            TimeSource.INGEST
        );
        assertEquals(TimeSource.INGEST, metric.timeSourceOrDefault());

        CompiledDerivedMetrics compiled = CompiledDerivedMetrics.compile(
            new DataStreamDerivedMetrics(true, List.of(), null, null, List.of(), List.of(metric))
        );
        assertEquals(TimeSource.INGEST, compiled.metrics().get(0).timeSource());
    }

    /**
     * The timestamp is stored in a keyed slot rather than among the document's fields, so it has to be fetched by key. A walk of the
     * document's fields, which is how every other value is read, would never find it.
     */
    public void testTheTimestampIsReadFromTheParsedDocument() throws IOException {
        MapperService mappers = createMapperService(MILLIS_MAPPING);
        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), List.of("service.name"));

        ParsedDocument document = parse(mappers, """
            {"@timestamp":"2026-01-01T00:00:00.000Z","service.name":"checkout"}""");
        assertEquals(1767225600000L, DerivedMetricsDocumentReader.timestampMillis(document, strategies));
    }

    /**
     * A date_nanos timestamp is stored as nanoseconds. Bucketing by it without converting would place the observation about a million
     * times further into the future than it belongs, so the two mappings must agree on the answer.
     */
    public void testANanosecondTimestampMeansTheSameMomentAsAMillisecondOne() throws IOException {
        MapperService millis = createMapperService(MILLIS_MAPPING);
        MapperService nanos = createMapperService(NANOS_MAPPING);

        String source = """
            {"@timestamp":"2026-01-01T00:00:00.000Z","service.name":"checkout"}""";
        long fromMillis = DerivedMetricsDocumentReader.timestampMillis(
            parse(millis, source),
            DerivedMetricsDocumentReader.resolve(millis.mappingLookup(), List.of("service.name"))
        );
        long fromNanos = DerivedMetricsDocumentReader.timestampMillis(
            parse(nanos, source),
            DerivedMetricsDocumentReader.resolve(nanos.mappingLookup(), List.of("service.name"))
        );
        assertEquals("the same instant, however the mapping stores it", fromMillis, fromNanos);
    }

    /**
     * A document with no timestamp this can read has to have an answer rather than an exception: some failure paths hand over no parsed
     * document at all, and the columnar parse does not record the timestamp yet.
     */
    public void testAMissingTimestampIsReportedRatherThanThrown() throws IOException {
        MapperService mappers = createMapperService(MILLIS_MAPPING);
        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), List.of("service.name"));

        assertEquals(DerivedMetricsDocumentReader.NO_TIMESTAMP, DerivedMetricsDocumentReader.timestampMillis(null, strategies));
    }

    /**
     * The two clocks have to reach different buckets from the same document, or none of this means anything. A metric on event time is
     * placed where the document says it happened; a metric on the write clock is placed where it was written.
     */
    public void testTheTwoClocksBucketTheSameDocumentDifferently() throws Exception {
        MapperService mappers = createMapperService(MILLIS_MAPPING);
        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), List.of("service.name"));
        // an hour old, far outside anything a lateness allowance would accept, so the difference is unmistakable
        long eventTime = System.currentTimeMillis() - TimeValue.timeValueHours(1).millis();
        ParsedDocument document = parse(
            mappers,
            "{\"@timestamp\":\"" + Instant.ofEpochMilli(eventTime) + "\",\"service.name\":\"checkout\"}"
        );

        assertEquals(eventTime, DerivedMetricsDocumentReader.timestampMillis(document, strategies));

        long interval = TimeValue.timeValueSeconds(10).millis();
        long eventBucket = DerivedMetricsBuffer.bucketStart(eventTime, interval);
        long writeBucket = DerivedMetricsBuffer.bucketStart(System.currentTimeMillis(), interval);
        assertNotEquals("an hour apart cannot share a ten second bucket", eventBucket, writeBucket);
    }

    /**
     * Bucket boundaries come from flooring the epoch, so every node agrees on them without coordinating and without a time zone entering
     * into it. Two documents a few milliseconds either side of a boundary belong to different buckets, wherever they were written.
     */
    public void testBucketsAreEpochAlignedAndSoAgreeEverywhere() {
        long interval = TimeValue.timeValueSeconds(10).millis();
        assertEquals(0L, DerivedMetricsBuffer.bucketStart(9_999L, interval));
        assertEquals(10_000L, DerivedMetricsBuffer.bucketStart(10_000L, interval));
        assertEquals(10_000L, DerivedMetricsBuffer.bucketStart(19_999L, interval));
    }

    private ParsedDocument parse(MapperService mappers, String source) throws IOException {
        return mappers.documentMapper().parse(source(source));
    }
}
