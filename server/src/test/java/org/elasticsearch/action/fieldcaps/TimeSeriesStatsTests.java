/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesStatsTests extends ESTestCase {

    public void testStatsWithBounds() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            boolean skipper = randomBoolean();
            addSegment(writer, skipper, 3, 1_000, 2_000);
            addSegment(writer, skipper, 2, 5_000, 9_000);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertThat(reader.leaves().size(), equalTo(2));
                DateFieldMapper.Resolution millis = DateFieldMapper.Resolution.MILLISECONDS;
                assertThat(
                    TimeSeriesStats.ofShard(reader, millis, 2, Long.MIN_VALUE, Long.MAX_VALUE),
                    equalTo(new TimeSeriesStats(2 * (3 * 1_001 + 2 * 4_001), 1_000, 9_000))
                );
                assertThat(
                    TimeSeriesStats.ofShard(reader, millis, 1, 1_500, 6_000),
                    equalTo(new TimeSeriesStats(3 * 501 + 2 * 1_001, 1_500, 6_000))
                );
                assertThat(
                    TimeSeriesStats.ofShard(reader, millis, 1, 8_000, Long.MAX_VALUE),
                    equalTo(new TimeSeriesStats(2 * 1_001, 8_000, 9_000))
                );
                assertThat(
                    TimeSeriesStats.ofShard(reader, millis, 1, 3_000, 4_000),
                    equalTo(new TimeSeriesStats(0, Long.MAX_VALUE, Long.MIN_VALUE))
                );
            }
        }
    }

    public void testSkipTimestampBounds() {
        DateFieldMapper.DateFieldType timestamp = new DateFieldMapper.DateFieldType(DataStream.TIMESTAMP_FIELD_NAME);
        long now = 1_000_000;
        assertArrayEquals(new long[] { Long.MIN_VALUE, Long.MAX_VALUE }, TimeSeriesStats.timestampBounds(null, timestamp, now));
        assertArrayEquals(
            new long[] { Long.MIN_VALUE, Long.MAX_VALUE },
            TimeSeriesStats.timestampBounds(new TermQueryBuilder("host", "a"), timestamp, now)
        );
        QueryBuilder filter = new BoolQueryBuilder().filter(new RangeQueryBuilder(DataStream.TIMESTAMP_FIELD_NAME).gte("now-1s").lt("now"))
            .filter(new BoolQueryBuilder().must(new RangeQueryBuilder(DataStream.TIMESTAMP_FIELD_NAME).gt(999_500)))
            .filter(new RangeQueryBuilder("other").gte(999_900))
            .should(new RangeQueryBuilder(DataStream.TIMESTAMP_FIELD_NAME).gte(999_900))
            .filter(new TermQueryBuilder("host", "a"));
        assertArrayEquals(new long[] { 999_501, 999_999 }, TimeSeriesStats.timestampBounds(filter, timestamp, now));
    }

    public void testTimestampBoundsWithFormatAndTimeZone() {
        DateFieldMapper.DateFieldType timestamp = new DateFieldMapper.DateFieldType(DataStream.TIMESTAMP_FIELD_NAME);
        QueryBuilder filter = new RangeQueryBuilder(DataStream.TIMESTAMP_FIELD_NAME).gte("2026-01-01")
            .lte("2026-01-02")
            .format("yyyy-MM-dd")
            .timeZone("+01:00");
        long from = Instant.parse("2025-12-31T23:00:00Z").toEpochMilli();
        long to = Instant.parse("2026-01-02T22:59:59.999Z").toEpochMilli();
        assertArrayEquals(new long[] { from, to }, TimeSeriesStats.timestampBounds(filter, timestamp, 0));
    }

    public void testDateNanos() {
        DateFieldMapper.DateFieldType timestamp = new DateFieldMapper.DateFieldType(
            DataStream.TIMESTAMP_FIELD_NAME,
            DateFieldMapper.Resolution.NANOSECONDS
        );
        QueryBuilder filter = new RangeQueryBuilder(DataStream.TIMESTAMP_FIELD_NAME).gt("1970-01-01T00:00:01.000000500Z")
            .lte("1970-01-01T00:00:02.000000500Z");
        assertArrayEquals(new long[] { 1_000, 2_001 }, TimeSeriesStats.timestampBounds(filter, timestamp, 0));
    }

    private static IndexWriter newWriter(Directory dir) throws IOException {
        return new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    }

    private static void addSegment(IndexWriter writer, boolean skipper, int numSeries, long minTimestamp, long maxTimestamp)
        throws IOException {
        for (int series = 0; series < numSeries; series++) {
            for (long timestamp : new long[] { minTimestamp, maxTimestamp }) {
                Document doc = new Document();
                doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, new BytesRef("tsid-" + series)));
                if (skipper) {
                    doc.add(SortedNumericDocValuesField.indexedField(DataStream.TIMESTAMP_FIELD_NAME, timestamp));
                } else {
                    doc.add(new LongPoint(DataStream.TIMESTAMP_FIELD_NAME, timestamp));
                }
                writer.addDocument(doc);
            }
        }
        writer.flush();
    }
}
