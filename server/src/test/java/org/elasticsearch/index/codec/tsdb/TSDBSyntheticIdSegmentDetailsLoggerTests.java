/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.logging.log4j.Level;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TIMESTAMP;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TSDBSyntheticIdSegmentDetailsLoggerTests extends ESTestCase {

    private static final long ONE_HOUR_MILLIS = TimeUnit.HOURS.toMillis(1);
    private static final long SIX_HOURS_MILLIS = TimeUnit.HOURS.toMillis(6);
    private static final long TWELVE_HOURS_MILLIS = TimeUnit.HOURS.toMillis(12);

    public void testLogsAtInfoLevelWhenTimestampRangeExceedsOneHour() {
        long timestampRange = ONE_HOUR_MILLIS + 1;
        assertSeenLogAtLevel(Level.INFO, timestampRange, "*TSDB segment [test_segment]*range=[" + timestampRange + "ms*");
    }

    public void testLogsAtWarnLevelWhenTimestampRangeExceedsSixHours() {
        long timestampRange = SIX_HOURS_MILLIS + 1;
        assertSeenLogAtLevel(Level.WARN, timestampRange, "*TSDB segment [test_segment]*range=[" + timestampRange + "ms*");
    }

    public void testLogsAtErrorLevelWhenTimestampRangeExceedsTwelveHours() {
        long timestampRange = TWELVE_HOURS_MILLIS + 1;
        assertSeenLogAtLevel(Level.ERROR, timestampRange, "*TSDB segment [test_segment]*range=[" + timestampRange + "ms*");
    }

    public void testNoInfoLogWhenTimestampRangeIsSmall() {
        long timestampRange = ONE_HOUR_MILLIS - 1;
        assertUnseenLogAtLevel(Level.INFO, timestampRange, "*TSDB segment*");
    }

    public void testNoInfoLogWhenBothTimestampAndTsidFieldsAreMissing() {
        var segmentState = createSegmentReadState("test_segment", IndexWriter.SOURCE_FLUSH, 100, false, false);
        var docValuesProducer = mockDocValuesProducerWithoutFields();

        MockLog.assertThatLogger(
            () -> new TSDBSyntheticIdSegmentDetailsLogger().maybeLogSegmentDetails(segmentState, docValuesProducer),
            TSDBSyntheticIdSegmentDetailsLogger.class,
            new MockLog.UnseenEventExpectation(
                "no info log when both fields are missing",
                TSDBSyntheticIdSegmentDetailsLogger.class.getCanonicalName(),
                Level.INFO,
                "*TSDB segment*"
            )
        );
    }

    private void assertSeenLogAtLevel(Level level, long timestampRange, String messagePattern) {
        runTest(
            timestampRange,
            new MockLog.SeenEventExpectation(
                level.name() + " log for timestamp range",
                TSDBSyntheticIdSegmentDetailsLogger.class.getCanonicalName(),
                level,
                messagePattern
            )
        );
    }

    private void assertUnseenLogAtLevel(Level level, long timestampRange, String messagePattern) {
        runTest(
            timestampRange,
            new MockLog.UnseenEventExpectation(
                "no " + level.name() + " log for timestamp range",
                TSDBSyntheticIdSegmentDetailsLogger.class.getCanonicalName(),
                level,
                messagePattern
            )
        );
    }

    private void runTest(long timestampRange, MockLog.LoggingExpectation expectation) {
        long minTimestamp = 1000L;
        long maxTimestamp = minTimestamp + timestampRange;

        var segmentState = createSegmentReadState("test_segment", IndexWriter.SOURCE_FLUSH, 100);
        var docValuesProducer = mockDocValuesProducer(minTimestamp, maxTimestamp, 10);

        MockLog.assertThatLogger(
            () -> new TSDBSyntheticIdSegmentDetailsLogger().maybeLogSegmentDetails(segmentState, docValuesProducer),
            TSDBSyntheticIdSegmentDetailsLogger.class,
            expectation
        );
    }

    private SegmentReadState createSegmentReadState(String segmentName, String source, int maxDoc) {
        return createSegmentReadState(segmentName, source, maxDoc, true, true);
    }

    private SegmentReadState createSegmentReadState(String segmentName, String source, int maxDoc, boolean hasTimestamp, boolean hasTsid) {
        Directory directory = mock(Directory.class);

        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            segmentName,
            maxDoc,
            false,
            false,
            null,
            Map.of(),
            new byte[16],
            Map.of(IndexWriter.SOURCE, source),
            null
        );

        int fieldNumber = 0;
        java.util.List<FieldInfo> fields = new java.util.ArrayList<>();
        if (hasTimestamp) {
            fields.add(
                new FieldInfo(
                    TIMESTAMP,
                    fieldNumber++,
                    false,
                    false,
                    false,
                    IndexOptions.NONE,
                    DocValuesType.SORTED_NUMERIC,
                    DocValuesSkipIndexType.NONE,
                    -1,
                    Collections.emptyMap(),
                    0,
                    0,
                    0,
                    0,
                    VectorEncoding.FLOAT32,
                    VectorSimilarityFunction.EUCLIDEAN,
                    false,
                    false
                )
            );
        }
        if (hasTsid) {
            fields.add(
                new FieldInfo(
                    TS_ID,
                    fieldNumber,
                    false,
                    false,
                    false,
                    IndexOptions.NONE,
                    DocValuesType.SORTED,
                    DocValuesSkipIndexType.NONE,
                    -1,
                    Collections.emptyMap(),
                    0,
                    0,
                    0,
                    0,
                    VectorEncoding.FLOAT32,
                    VectorSimilarityFunction.EUCLIDEAN,
                    false,
                    false
                )
            );
        }
        FieldInfos fieldInfos = new FieldInfos(fields.toArray(new FieldInfo[0]));

        return new SegmentReadState(directory, segmentInfo, fieldInfos, null, null);
    }

    private DocValuesProducer mockDocValuesProducer(long minTimestamp, long maxTimestamp, long tsidOrdinalCount) {
        try {
            DocValuesSkipper timestampSkipper = mock(DocValuesSkipper.class);
            when(timestampSkipper.minValue()).thenReturn(minTimestamp);
            when(timestampSkipper.maxValue()).thenReturn(maxTimestamp);

            SortedDocValues tsidDocValues = mock(SortedDocValues.class);
            when(tsidDocValues.getValueCount()).thenReturn((int) tsidOrdinalCount);

            DocValuesProducer producer = mock(DocValuesProducer.class);
            when(producer.getSkipper(org.mockito.ArgumentMatchers.any())).thenReturn(timestampSkipper);
            when(producer.getSorted(org.mockito.ArgumentMatchers.any())).thenReturn(tsidDocValues);

            return producer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DocValuesProducer mockDocValuesProducerWithoutFields() {
        try {
            DocValuesProducer producer = mock(DocValuesProducer.class);
            when(producer.getSkipper(org.mockito.ArgumentMatchers.any())).thenReturn(null);
            when(producer.getSorted(org.mockito.ArgumentMatchers.any())).thenReturn(null);

            return producer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
