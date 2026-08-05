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
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TIMESTAMP;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TSDBSyntheticIdSegmentDetailsLoggerTests extends ESTestCase {

    private static final String LOGGER_NAME = "org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdSegmentDetailsLogger";

    @TestLogging(reason = "testing DEBUG logging", value = LOGGER_NAME + ":DEBUG")
    public void testLogsAtDebugLevelForFlushedSegments() {
        var segmentState = createSegmentReadState("test_segment", IndexWriter.SOURCE_FLUSH, 100);
        var docValuesProducer = mockDocValuesProducer(1000L, 2000L, 10);

        try (var mockLog = MockLog.capture(TSDBSyntheticIdSegmentDetailsLogger.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "DEBUG log for flushed segment",
                    LOGGER_NAME,
                    Level.DEBUG,
                    "*TSDB segment [test_segment]*source=[flush]*"
                )
            );
            TSDBSyntheticIdSegmentDetailsLogger.maybeLogSegmentDetails(segmentState, docValuesProducer);
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(reason = "testing TRACE logging", value = LOGGER_NAME + ":TRACE")
    public void testLogsAtTraceLevelForMergedSegments() {
        var segmentState = createSegmentReadState("test_segment", IndexWriter.SOURCE_MERGE, 100);
        var docValuesProducer = mockDocValuesProducer(1000L, 2000L, 10);

        try (var mockLog = MockLog.capture(TSDBSyntheticIdSegmentDetailsLogger.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "TRACE log for merged segment",
                    LOGGER_NAME,
                    Level.TRACE,
                    "*TSDB segment [test_segment]*source=[merge]*"
                )
            );
            TSDBSyntheticIdSegmentDetailsLogger.maybeLogSegmentDetails(segmentState, docValuesProducer);
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(reason = "testing DEBUG logging does not include merged segments", value = LOGGER_NAME + ":DEBUG")
    public void testNoDebugLogForMergedSegments() {
        var segmentState = createSegmentReadState("test_segment", IndexWriter.SOURCE_MERGE, 100);
        var docValuesProducer = mockDocValuesProducer(1000L, 2000L, 10);

        try (var mockLog = MockLog.capture(TSDBSyntheticIdSegmentDetailsLogger.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no DEBUG log for merged segment", LOGGER_NAME, Level.DEBUG, "*TSDB segment*")
            );
            TSDBSyntheticIdSegmentDetailsLogger.maybeLogSegmentDetails(segmentState, docValuesProducer);
            mockLog.assertAllExpectationsMatched();
        }
    }

    private SegmentReadState createSegmentReadState(String segmentName, String source, int maxDoc) {
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
            Map.of(IndexWriter.SOURCE, source),
            new byte[16],
            Map.of(),
            null
        );

        java.util.List<FieldInfo> fields = new java.util.ArrayList<>();
        fields.add(
            new FieldInfo(
                TIMESTAMP,
                0,
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
        fields.add(
            new FieldInfo(
                TS_ID,
                1,
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
}
