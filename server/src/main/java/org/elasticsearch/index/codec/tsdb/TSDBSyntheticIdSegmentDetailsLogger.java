/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TIMESTAMP;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ID;

/**
 * Logger for TSDB segment details. Logs diagnostic information about segments when they are read,
 * including timestamp range and tsid statistics.
 *
 * <p>The log level is selected based on the segment source:</p>
 * <ul>
 *     <li>DEBUG: flushed segments only</li>
 *     <li>TRACE: both flushed and merged segments</li>
 * </ul>
 *
 * <p>Logged information includes: segment name, source (flush/merge), maxDoc,
 * timestamp min/max/range, tsid ordinal count, and tsid uniqueness ratio.</p>
 */
class TSDBSyntheticIdSegmentDetailsLogger {

    private static final Logger logger = LogManager.getLogger(TSDBSyntheticIdSegmentDetailsLogger.class);

    private static final String NO_SOURCE = "none";

    /**
     * Logs segment details at DEBUG level for flushed segments, TRACE level for merged segments.
     * <p>
     * If the timestamp or tsid fields are missing, their values are logged as -1 or 0 respectively.
     *
     * @param state the segment read state containing segment info and field infos
     * @param docValuesProducer the doc values producer to read timestamp and tsid values
     */
    public static void maybeLogSegmentDetails(SegmentReadState state, DocValuesProducer docValuesProducer) {
        final boolean isTraceEnabled = logger.isTraceEnabled();
        if (isTraceEnabled == false && logger.isDebugEnabled() == false) {
            return;
        }

        final String source;
        var diagnostics = state.segmentInfo.getDiagnostics();
        if (diagnostics != null) {
            source = state.segmentInfo.getDiagnostics().get(IndexWriter.SOURCE);
        } else {
            source = NO_SOURCE;
        }

        final boolean isSourceFlush = IndexWriter.SOURCE_FLUSH.equals(source);
        if (isTraceEnabled || isSourceFlush) {
            try {
                SegmentInfo segmentInfo = state.segmentInfo;
                int maxDoc = segmentInfo.maxDoc();

                FieldInfo timestampFieldInfo = state.fieldInfos.fieldInfo(TIMESTAMP);
                FieldInfo tsIdFieldInfo = state.fieldInfos.fieldInfo(TS_ID);

                final long minTimestamp;
                final long maxTimestamp;
                final long timestampRangeMillis;
                final long tsIdOrdinalCount;
                final double tsidUniquenessRatio;

                if (timestampFieldInfo != null) {
                    DocValuesSkipper timestampSkipper = docValuesProducer.getSkipper(timestampFieldInfo);
                    if (timestampSkipper != null) {
                        minTimestamp = timestampSkipper.minValue();
                        maxTimestamp = timestampSkipper.maxValue();
                        timestampRangeMillis = maxTimestamp - minTimestamp;
                    } else {
                        timestampRangeMillis = 0;
                        maxTimestamp = -1;
                        minTimestamp = -1;
                    }
                } else {
                    timestampRangeMillis = 0;
                    maxTimestamp = -1;
                    minTimestamp = -1;
                }

                if (tsIdFieldInfo != null) {
                    SortedDocValues tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
                    if (tsIdDocValues != null) {
                        tsIdOrdinalCount = tsIdDocValues.getValueCount();
                        if (maxDoc > 0) {
                            tsidUniquenessRatio = (double) tsIdOrdinalCount / maxDoc;
                        } else {
                            tsidUniquenessRatio = 0.0;
                        }
                    } else {
                        tsidUniquenessRatio = 0.0;
                        tsIdOrdinalCount = 0;
                    }
                } else {
                    tsidUniquenessRatio = 0.0;
                    tsIdOrdinalCount = 0;
                }

                TimeValue timestampRange = TimeValue.timeValueMillis(timestampRangeMillis);
                Supplier<String> logMessage = () -> String.format(
                    Locale.ROOT,
                    "TSDB segment [%s]: source=[%s], maxDocs=[%d], "
                        + "timestamp min=[%d] max=[%d] range=[%dms / %s], "
                        + "tsid ordinals=[%d], tsid uniqueness ratio=[%.4f]",
                    segmentInfo.name,
                    source,
                    maxDoc,
                    minTimestamp,
                    maxTimestamp,
                    timestampRangeMillis,
                    timestampRange,
                    tsIdOrdinalCount,
                    tsidUniquenessRatio
                );

                if (isSourceFlush) {
                    logger.debug(logMessage);
                } else {
                    logger.trace(logMessage);
                }
            } catch (Exception e) {
                logger.warn("Failed to log segment info", e);
            }
        }
    }
}
