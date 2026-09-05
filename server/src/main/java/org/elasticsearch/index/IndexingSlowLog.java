/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StringBuilders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class IndexingSlowLog implements IndexingOperationListener {
    public static final String INDEX_INDEXING_SLOWLOG_PREFIX = "index.indexing.slowlog";
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING = Setting.timeSetting(
        INDEX_INDEXING_SLOWLOG_PREFIX + ".threshold.index.warn",
        TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING = Setting.timeSetting(
        INDEX_INDEXING_SLOWLOG_PREFIX + ".threshold.index.info",
        TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING = Setting.timeSetting(
        INDEX_INDEXING_SLOWLOG_PREFIX + ".threshold.index.debug",
        TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING = Setting.timeSetting(
        INDEX_INDEXING_SLOWLOG_PREFIX + ".threshold.index.trace",
        TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Boolean> INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING = Setting.boolSetting(
        INDEX_INDEXING_SLOWLOG_PREFIX + ".reformat",
        true,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Boolean> INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING = Setting.boolSetting(
        INDEX_INDEXING_SLOWLOG_PREFIX + ".include.user",
        false,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Legacy index setting, kept for 7.x BWC compatibility. This setting has no effect in 8.x. Do not use.
     * TODO: Remove in 9.0
     */
    @Deprecated
    public static final Setting<String> INDEX_INDEXING_SLOWLOG_LEVEL_SETTING = new Setting<>(
        INDEX_INDEXING_SLOWLOG_PREFIX + ".level",
        "",
        (s) -> s,
        Property.Dynamic,
        Property.IndexScope,
        Property.IndexSettingDeprecatedInV7AndRemovedInV8
    );

    private static final Logger indexLogger = LogManager.getLogger(INDEX_INDEXING_SLOWLOG_PREFIX + ".index");

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
    private final ActionLoggingFields loggingFields;

    /**
     * Reads how much of the source to log. The user can specify any value they
     * like and numbers are interpreted the maximum number of characters to log
     * and everything else is interpreted as Elasticsearch interprets booleans
     * which is then converted to 0 for false and Integer.MAX_VALUE for true.
     */
    public static final Setting<Integer> INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING = new Setting<>(
        INDEX_INDEXING_SLOWLOG_PREFIX + ".source",
        "1000",
        (value) -> {
            try {
                return Integer.parseInt(value, 10);
            } catch (NumberFormatException e) {
                return Booleans.parseBoolean(value, true) ? Integer.MAX_VALUE : 0;
            }
        },
        Property.Dynamic,
        Property.IndexScope
    );

    IndexingSlowLog(IndexSettings indexSettings, ActionLoggingFieldsProvider slowLogFieldsProvider) {
        this.index = indexSettings.getIndex();

        ActionLoggingFieldsContext logContext = new ActionLoggingFieldsContext(
            indexSettings.getValue(INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING)
        );
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING, logContext::setIncludeUserInformation);

        this.loggingFields = slowLogFieldsProvider.create(logContext);

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
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING, this::setMaxSourceCharsToLog);
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
            logIfSlow(
                tookInNanos,
                () -> IndexingSlowLogMessage.of(loggingFields.logFields(), index, doc, tookInNanos, reformat, maxSourceCharsToLog)
            );
        }
    }

    @Override
    public IndexOperationBatch preIndexBatch(ShardId shardId, IndexOperationBatch batch) {
        // no pre-work; overridden to avoid the delegating to default
        return batch;
    }

    /**
     * Batch equivalent of {@link #postIndex(ShardId, Engine.Index, Engine.IndexResult)} call.
     * This method considers the time taken by the batch as the average time for successful operations within a batch.
     */
    @Override
    public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, List<Engine.IndexResult> results) {
        long totalTook = 0;
        int successCount = 0;
        for (Engine.IndexResult result : results) {
            if (result.getResultType() == Engine.Result.Type.SUCCESS) {
                totalTook += result.getTook();
                successCount++;
            }
        }
        if (successCount == 0) return;
        final long avgTook = totalTook / successCount;
        final long startingSeqNo = batch.seqNo(0);
        final int docCount = batch.docCount();
        final int finalSuccessCount = successCount;
        logIfSlow(
            avgTook,
            () -> IndexingSlowLogMessage.ofBatch(loggingFields.logFields(), index, startingSeqNo, docCount, finalSuccessCount, avgTook)
        );
    }

    @Override
    public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, Exception ex) {
        // engine level failures are never slow-logged, mirroring the per-op hooks
    }

    private void logIfSlow(long tookInNanos, Supplier<ESLogMessage> messageProducer) {
        if (indexWarnThreshold >= 0 && tookInNanos > indexWarnThreshold) {
            indexLogger.warn(messageProducer.get());
        } else if (indexInfoThreshold >= 0 && tookInNanos > indexInfoThreshold) {
            indexLogger.info(messageProducer.get());
        } else if (indexDebugThreshold >= 0 && tookInNanos > indexDebugThreshold) {
            indexLogger.debug(messageProducer.get());
        } else if (indexTraceThreshold >= 0 && tookInNanos > indexTraceThreshold) {
            indexLogger.trace(messageProducer.get());
        }
    }

    static final class IndexingSlowLogMessage {

        public static ESLogMessage of(
            Map<String, String> additionalFields,
            Index index,
            ParsedDocument doc,
            long tookInNanos,
            boolean reformat,
            int maxSourceCharsToLog
        ) {

            Map<String, Object> jsonFields = prepareMap(index, doc, tookInNanos, reformat, maxSourceCharsToLog);
            jsonFields.putAll(additionalFields);
            return new ESLogMessage().withFields(jsonFields);
        }

        public static ESLogMessage ofBatch(
            Map<String, String> additionalFields,
            Index index,
            long startingSeqNo,
            int docCount,
            int successCount,
            long avgTookInNanos
        ) {
            Map<String, Object> map = new HashMap<>();
            map.put("elasticsearch.slowlog.message", index);
            map.put("elasticsearch.slowlog.took", TimeValue.timeValueNanos(avgTookInNanos).toString());
            map.put("elasticsearch.slowlog.took_millis", String.valueOf(TimeUnit.NANOSECONDS.toMillis(avgTookInNanos)));
            map.put("elasticsearch.slowlog.starting_seq_no", startingSeqNo);
            map.put("elasticsearch.slowlog.doc_count", docCount);
            map.put("elasticsearch.slowlog.success_count", successCount);
            map.putAll(additionalFields);
            return new ESLogMessage().withFields(map);
        }

        private static Map<String, Object> prepareMap(
            Index index,
            ParsedDocument doc,
            long tookInNanos,
            boolean reformat,
            int maxSourceCharsToLog
        ) {
            Map<String, Object> map = new HashMap<>();
            map.put("elasticsearch.slowlog.message", index);
            map.put("elasticsearch.slowlog.took", TimeValue.timeValueNanos(tookInNanos).toString());
            map.put("elasticsearch.slowlog.took_millis", String.valueOf(TimeUnit.NANOSECONDS.toMillis(tookInNanos)));
            map.put("elasticsearch.slowlog.id", doc.id());
            if (doc.routing() != null) {
                map.put("elasticsearch.slowlog.routing", doc.routing());
            }

            SourceToParse.Source sourceObject = doc.source();
            // TODO: Will materialize to original x-content if rows. Consider if we eventually want to optimize this.
            if (maxSourceCharsToLog == 0 || sourceObject == null || sourceObject.originalBytes().length() == 0) {
                return map;
            }
            try {
                String source = XContentHelper.convertToJson(sourceObject.originalBytes(), reformat, sourceObject.xContentType());
                String trim = Strings.cleanTruncate(source, maxSourceCharsToLog).trim();
                StringBuilder sb = new StringBuilder(trim);
                StringBuilders.escapeJson(sb, 0);
                map.put("elasticsearch.slowlog.source", sb.toString());
            } catch (IOException e) {
                StringBuilder sb = new StringBuilder("_failed_to_convert_[" + e.getMessage() + "]");
                StringBuilders.escapeJson(sb, 0);
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

}
