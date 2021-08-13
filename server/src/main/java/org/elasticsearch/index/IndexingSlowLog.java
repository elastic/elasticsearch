/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StringBuilders;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class IndexingSlowLog implements IndexingOperationListener {
    public static final String INDEX_INDEXING_SLOWLOG_PREFIX = "index.indexing.slowlog";
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.warn", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.info", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.debug", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.trace", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<Boolean> INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING =
        Setting.boolSetting(INDEX_INDEXING_SLOWLOG_PREFIX +".reformat", true, Property.Dynamic, Property.IndexScope);

    private final Logger indexLogger;
    private final Index index;

    private boolean reformat;
    private long indexWarnThreshold;
    private long indexInfoThreshold;
    private long indexDebugThreshold;
    private long indexTraceThreshold;
    /*
     * How much of the source to log in the slowlog - 0 means log none and anything greater than 0 means log at least that many
     * <em>characters</em> of the source.
     */
    private int maxSourceCharsToLog;

    /**
     * Reads how much of the source to log. The user can specify any value they
     * like and numbers are interpreted the maximum number of characters to log
     * and everything else is interpreted as Elasticsearch interprets booleans
     * which is then converted to 0 for false and Integer.MAX_VALUE for true.
     */
    public static final Setting<Integer> INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING =
            new Setting<>(INDEX_INDEXING_SLOWLOG_PREFIX + ".source", "1000", (value) -> {
                try {
                    return Integer.parseInt(value, 10);
                } catch (NumberFormatException e) {
                    return Booleans.parseBoolean(value, true) ? Integer.MAX_VALUE : 0;
                }
            }, Property.Dynamic, Property.IndexScope);

    IndexingSlowLog(IndexSettings indexSettings) {
        this.indexLogger = LogManager.getLogger(INDEX_INDEXING_SLOWLOG_PREFIX + ".index");
        Loggers.setLevel(this.indexLogger, Level.TRACE);
        this.index = indexSettings.getIndex();

        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING, this::setReformat);
        this.reformat = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING);
        indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING, this::setWarnThreshold);
        this.indexWarnThreshold = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING).nanos();
        indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING, this::setInfoThreshold);
        this.indexInfoThreshold = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING).nanos();
        indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING, this::setDebugThreshold);
        this.indexDebugThreshold = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING).nanos();
        indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING, this::setTraceThreshold);
        this.indexTraceThreshold = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING).nanos();
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING,
                this::setMaxSourceCharsToLog);
        this.maxSourceCharsToLog = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING);
    }

    private void setMaxSourceCharsToLog(int maxSourceCharsToLog) {
        this.maxSourceCharsToLog = maxSourceCharsToLog;
    }

    private void setWarnThreshold(TimeValue warnThreshold) {
        this.indexWarnThreshold = warnThreshold.nanos();
    }

    private void setInfoThreshold(TimeValue infoThreshold) {
        this.indexInfoThreshold = infoThreshold.nanos();
    }

    private void setDebugThreshold(TimeValue debugThreshold) {
        this.indexDebugThreshold = debugThreshold.nanos();
    }

    private void setTraceThreshold(TimeValue traceThreshold) {
        this.indexTraceThreshold = traceThreshold.nanos();
    }

    private void setReformat(boolean reformat) {
        this.reformat = reformat;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index indexOperation, Engine.IndexResult result) {
        if (result.getResultType() == Engine.Result.Type.SUCCESS) {
            final ParsedDocument doc = indexOperation.parsedDoc();
            final long tookInNanos = result.getTook();
            if (indexWarnThreshold >= 0 && tookInNanos > indexWarnThreshold) {
                indexLogger.warn(IndexingSlowLogMessage.of(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            } else if (indexInfoThreshold >= 0 && tookInNanos > indexInfoThreshold) {
                indexLogger.info(IndexingSlowLogMessage.of(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            } else if (indexDebugThreshold >= 0 && tookInNanos > indexDebugThreshold) {
                indexLogger.debug(IndexingSlowLogMessage.of(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            } else if (indexTraceThreshold >= 0 && tookInNanos > indexTraceThreshold) {
                indexLogger.trace(IndexingSlowLogMessage.of(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            }
        }
    }

    static final class IndexingSlowLogMessage {

        public static ESLogMessage of(
            Index index, ParsedDocument doc, long tookInNanos, boolean reformat, int maxSourceCharsToLog) {

            Map<String, Object> jsonFields = prepareMap(index, doc, tookInNanos, reformat, maxSourceCharsToLog);
            return new ESLogMessage().withFields(jsonFields);
        }

        private static Map<String, Object> prepareMap(Index index, ParsedDocument doc, long tookInNanos, boolean reformat,
                                                      int maxSourceCharsToLog) {
            Map<String,Object> map = new HashMap<>();
            map.put("elasticsearch.slowlog.message", index);
            map.put("elasticsearch.slowlog.took", TimeValue.timeValueNanos(tookInNanos).toString());
            map.put("elasticsearch.slowlog.took_millis", String.valueOf(TimeUnit.NANOSECONDS.toMillis(tookInNanos)));
            map.put("elasticsearch.slowlog.id", doc.id());
            if (doc.routing() != null) {
                map.put("elasticsearch.slowlog.routing", doc.routing());
            }

            if (maxSourceCharsToLog == 0 || doc.source() == null || doc.source().length() == 0) {
                return map;
            }
            try {
                String source = XContentHelper.convertToJson(doc.source(), reformat, doc.getXContentType());
                String trim = Strings.cleanTruncate(source, maxSourceCharsToLog).trim();
                StringBuilder sb  = new StringBuilder(trim);
                StringBuilders.escapeJson(sb,0);
                map.put("elasticsearch.slowlog.source", sb.toString());
            } catch (IOException e) {
                StringBuilder sb  = new StringBuilder("_failed_to_convert_[" + e.getMessage()+"]");
                StringBuilders.escapeJson(sb,0);
                map.put("elasticsearch.slowlog.source", sb.toString());
                /*
                 * We choose to fail to write to the slow log and instead let this percolate up to the post index listener loop where this
                 * will be logged at the warn level.
                 */
                final String message = String.format(Locale.ROOT, "failed to convert source for slow log entry [%s]", map.toString());
                throw new UncheckedIOException(message, e);
            }
            return map;
        }
    }

    boolean isReformat() {
        return reformat;
    }

    long getIndexWarnThreshold() {
        return indexWarnThreshold;
    }

    long getIndexInfoThreshold() {
        return indexInfoThreshold;
    }

    long getIndexTraceThreshold() {
        return indexTraceThreshold;
    }

    long getIndexDebugThreshold() {
        return indexDebugThreshold;
    }

    int getMaxSourceCharsToLog() {
        return maxSourceCharsToLog;
    }

}
