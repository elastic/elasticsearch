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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
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
 * <p>The log level is selected based on the segment's timestamp range (max - min):</p>
 * <ul>
 *     <li>ERROR: timestamp range &gt; error threshold (default 12 hours)</li>
 *     <li>WARN: timestamp range &gt; warn threshold (default 6 hours)</li>
 *     <li>INFO: timestamp range &gt; info threshold (default 1 hour)</li>
 *     <li>DEBUG: flushed segments with timestamp range below info threshold</li>
 *     <li>TRACE: merged segments with timestamp range below info threshold</li>
 * </ul>
 *
 * <p>Thresholds are configurable via dynamic index settings:</p>
 * <ul>
 *     <li>{@code index.time_series.segment_details_logger.info_threshold}</li>
 *     <li>{@code index.time_series.segment_details_logger.warn_threshold}</li>
 *     <li>{@code index.time_series.segment_details_logger.error_threshold}</li>
 * </ul>
 *
 * <p>Logged information includes: segment name, source (flush/merge), maxDoc,
 * timestamp min/max/range, tsid ordinal count, and tsid uniqueness ratio.</p>
 */
public class TSDBSyntheticIdSegmentDetailsLogger {

    private static final Logger logger = LogManager.getLogger(TSDBSyntheticIdSegmentDetailsLogger.class);

    public static final Setting<TimeValue> INFO_THRESHOLD_SETTING = Setting.timeSetting(
        "index.time_series.segment_details_logger.info_threshold",
        TimeValue.timeValueHours(1),
        TimeValue.ZERO,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> WARN_THRESHOLD_SETTING = Setting.timeSetting(
        "index.time_series.segment_details_logger.warn_threshold",
        TimeValue.timeValueHours(6),
        TimeValue.ZERO,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> ERROR_THRESHOLD_SETTING = Setting.timeSetting(
        "index.time_series.segment_details_logger.error_threshold",
        TimeValue.timeValueHours(12),
        TimeValue.ZERO,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    private volatile TimeValue infoThreshold;
    private volatile TimeValue warnThreshold;
    private volatile TimeValue errorThreshold;

    public TSDBSyntheticIdSegmentDetailsLogger() {
        this(null);
    }

    public TSDBSyntheticIdSegmentDetailsLogger(@Nullable IndexSettings indexSettings) {
        if (indexSettings != null) {
            this.infoThreshold = INFO_THRESHOLD_SETTING.get(indexSettings.getSettings());
            this.warnThreshold = WARN_THRESHOLD_SETTING.get(indexSettings.getSettings());
            this.errorThreshold = ERROR_THRESHOLD_SETTING.get(indexSettings.getSettings());
            indexSettings.getScopedSettings().addSettingsUpdateConsumer(INFO_THRESHOLD_SETTING, v -> this.infoThreshold = v);
            indexSettings.getScopedSettings().addSettingsUpdateConsumer(WARN_THRESHOLD_SETTING, v -> this.warnThreshold = v);
            indexSettings.getScopedSettings().addSettingsUpdateConsumer(ERROR_THRESHOLD_SETTING, v -> this.errorThreshold = v);
        } else {
            this.infoThreshold = INFO_THRESHOLD_SETTING.getDefault(null);
            this.warnThreshold = WARN_THRESHOLD_SETTING.getDefault(null);
            this.errorThreshold = ERROR_THRESHOLD_SETTING.getDefault(null);
        }
    }

    /**
     * Logs segment details at a level determined by the timestamp range.
     * <p>
     * If the timestamp or tsid fields are missing, their values are logged as -1 or 0 respectively.
     *
     * @param state the segment read state containing segment info and field infos
     * @param docValuesProducer the doc values producer to read timestamp and tsid values
     */
    public void maybeLogSegmentDetails(SegmentReadState state, DocValuesProducer docValuesProducer) {
        try {
            String source = state.segmentInfo.getDiagnostics().get(IndexWriter.SOURCE);
            boolean isFlushed = IndexWriter.SOURCE_FLUSH.equals(source);

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
                "TSDB segment [%s]: source=[%s], maxDoc=[%d], "
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

            if (timestampRangeMillis > errorThreshold.millis()) {
                logger.error(logMessage);
            } else if (timestampRangeMillis > warnThreshold.millis()) {
                logger.warn(logMessage);
            } else if (timestampRangeMillis > infoThreshold.millis()) {
                logger.info(logMessage);
            } else if (isFlushed) {
                logger.debug(logMessage);
            } else {
                logger.trace(logMessage);
            }
        } catch (Exception e) {
            logger.warn("Failed to log segment info", e);
        }
    }
}
