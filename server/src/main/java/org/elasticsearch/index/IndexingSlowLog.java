/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StringBuilders;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;
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
    private final Index index;
    private boolean reformat;
    private long indexWarnThreshold;
    private long indexInfoThreshold;
    private long indexDebugThreshold;
    private long indexTraceThreshold;
    /**
     * How much of the source to log in the slowlog - 0 means log none and
     * anything greater than 0 means log at least that many <em>characters</em>
     * of the source.
     */
    private int maxSourceCharsToLog;

    private SlowLogLevel level;

    private final Logger indexLogger;

    private static final String INDEX_INDEXING_SLOWLOG_PREFIX = "index.indexing.slowlog";
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX + ".threshold.index.warn", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX + ".threshold.index.info", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX + ".threshold.index.debug", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX + ".threshold.index.trace", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<Boolean> INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING =
        Setting.boolSetting(INDEX_INDEXING_SLOWLOG_PREFIX + ".reformat", true, Property.Dynamic, Property.IndexScope);
    public static final Setting<SlowLogLevel> INDEX_INDEXING_SLOWLOG_LEVEL_SETTING =
        new Setting<>(INDEX_INDEXING_SLOWLOG_PREFIX + ".level", SlowLogLevel.TRACE.name(), SlowLogLevel::parse, Property.Dynamic,
            Property.IndexScope);
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
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_LEVEL_SETTING, this::setLevel);
        setLevel(indexSettings.getValue(INDEX_INDEXING_SLOWLOG_LEVEL_SETTING));
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING,
            this::setMaxSourceCharsToLog);
        this.maxSourceCharsToLog = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING);
    }

    private void setMaxSourceCharsToLog(int maxSourceCharsToLog) {
        this.maxSourceCharsToLog = maxSourceCharsToLog;
    }

    private void setLevel(SlowLogLevel level) {
        this.level = level;
        Loggers.setLevel(this.indexLogger, level.name());
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
                indexLogger.warn( new SlowLogParsedDocumentPrinter(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            } else if (indexInfoThreshold >= 0 && tookInNanos > indexInfoThreshold) {
                indexLogger.info(new SlowLogParsedDocumentPrinter(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            } else if (indexDebugThreshold >= 0 && tookInNanos > indexDebugThreshold) {
                indexLogger.debug(new SlowLogParsedDocumentPrinter(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            } else if (indexTraceThreshold >= 0 && tookInNanos > indexTraceThreshold) {
                indexLogger.trace( new SlowLogParsedDocumentPrinter(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            }
        }
    }

    static final class SlowLogParsedDocumentPrinter extends LoggerMessage {
        private final ParsedDocument doc;
        private final long tookInNanos;
        private final boolean reformat;
        private final int maxSourceCharsToLog;
        private final Index index;

        SlowLogParsedDocumentPrinter(Index index, ParsedDocument doc, long tookInNanos, boolean reformat, int maxSourceCharsToLog) {
            super(prepareMap(index,doc,tookInNanos,reformat,maxSourceCharsToLog), createToString(index,doc,tookInNanos,reformat,maxSourceCharsToLog));
            this.doc = doc;
            this.index = index;
            this.tookInNanos = tookInNanos;
            this.reformat = reformat;
            this.maxSourceCharsToLog = maxSourceCharsToLog;
        }

        private static Map<String, Object> prepareMap(Index index, ParsedDocument doc, long tookInNanos, boolean reformat, int maxSourceCharsToLog) {
            Map<String,Object> map = new HashMap<>();
            map.put("message", inQuotes(index));
            map.put("took", inQuotes(TimeValue.timeValueNanos(tookInNanos)));
            map.put("took_millis", inQuotes(TimeUnit.NANOSECONDS.toMillis(tookInNanos)));
            map.put("doc_type", inQuotes(doc.type()));
            map.put("id", inQuotes(doc.id()));
            map.put("routing", inQuotes(doc.routing()));

            if (maxSourceCharsToLog == 0 || doc.source() == null || doc.source().length() == 0) {
                return map;
            }
            try {
                String source = XContentHelper.convertToJson(doc.source(), reformat, doc.getXContentType());
                String trim = Strings.cleanTruncate(source, maxSourceCharsToLog).trim();
                StringBuilder sb  = new StringBuilder(trim);
                StringBuilders.escapeJson(sb,0);
                map.put("source", inQuotes(sb.toString()));
            } catch (IOException e) {
                StringBuilder sb  = new StringBuilder("_failed_to_convert_[" + e.getMessage()+"]");
                StringBuilders.escapeJson(sb,0);
                map.put("source", inQuotes(sb.toString()));
                /*
                 * We choose to fail to write to the slow log and instead let this percolate up to the post index listener loop where this
                 * will be logged at the warn level.
                 */
                final String message = String.format(Locale.ROOT, "failed to convert source for slow log entry [%s]", map.toString());
                throw new UncheckedIOException(message, e);
            }
            return map;
        }

        @Override
        public String toString() {
            return createToString(index,doc,tookInNanos,reformat,maxSourceCharsToLog);
        }

        private static String createToString(Index index, ParsedDocument doc, long tookInNanos, boolean reformat, int maxSourceCharsToLog) {
            StringBuilder sb = new StringBuilder();
            sb.append(index).append(" ");
            sb.append("took[").append(TimeValue.timeValueNanos(tookInNanos)).append("], ");
            sb.append("took_millis[").append(TimeUnit.NANOSECONDS.toMillis(tookInNanos)).append("], ");
            sb.append("type[").append(doc.type()).append("], ");
            sb.append("id[").append(doc.id()).append("], ");
            if (doc.routing() == null) {
                sb.append("routing[]");
            } else {
                sb.append("routing[").append(doc.routing()).append("]");
            }

            if (maxSourceCharsToLog == 0 || doc.source() == null || doc.source().length() == 0) {
                return sb.toString();
            }
            try {
                String source = XContentHelper.convertToJson(doc.source(), reformat, doc.getXContentType());
                sb.append(", source[").append(Strings.cleanTruncate(source, maxSourceCharsToLog).trim()).append("]");
            } catch (IOException e) {
                sb.append(", source[_failed_to_convert_[").append(e.getMessage()).append("]]");
                /*
                 * We choose to fail to write to the slow log and instead let this percolate up to the post index listener loop where this
                 * will be logged at the warn level.
                 */
                final String message = String.format(Locale.ROOT, "failed to convert source for slow log entry [%s]", sb.toString());
                throw new UncheckedIOException(message, e);
            }
            return sb.toString();
        }

//        private String keyValue(String key, Object value) {
//            return wrapWithQuotes(key) + ": " + wrapWithQuotes(value);
//        }
//
//        private String wrapWithQuotes(Object value) {
//            if (value == null)
//                return "\"\"";
//            return "\"" + value + "\"";
//        }

//        private String keyValue(String key, String[] value) {
//            return keyValue(key, asList(value));
//        }
//
//        private String keyValue(String key, Collection<String> value) {
//            String array;
//            if (value == null) {
//                array = "";
//            } else {
//                array = value.stream().map(s -> wrapWithQuotes(s)).collect(Collectors.joining(", "));
//            }
//            return "\"" + key + "\": [" + array + "]";
//        }

//        @Override
//        public String getFormattedMessage() {
//            StringJoiner sj = new StringJoiner(",");
//            sj.add(keyValue("message", index));
//            sj.add(keyValue("took", TimeValue.timeValueNanos(tookInNanos)));
//            sj.add(keyValue("took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos)));
//            sj.add(keyValue("type", doc.type()));
//            sj.add(keyValue("id", doc.id()));
//            sj.add(keyValue("routing", doc.routing()));
//
//            if (maxSourceCharsToLog == 0 || doc.source() == null || doc.source().length() == 0) {
//                return sj.toString();
//            }
//            try {
//                String source = XContentHelper.convertToJson(doc.source(), reformat, doc.getXContentType());
//                sj.add(keyValue("source", Strings.cleanTruncate(source, maxSourceCharsToLog).trim()));
//            } catch (IOException e) {
//                sj.add(keyValue("source", "_failed_to_convert_[" + e.getMessage()+"]"));
//                /*
//                 * We choose to fail to write to the slow log and instead let this percolate up to the post index listener loop where this
//                 * will be logged at the warn level.
//                 */
//                final String message = String.format(Locale.ROOT, "failed to convert source for slow log entry [%s]", sj.toString());
//                throw new UncheckedIOException(message, e);
//            }
//            return sj.toString();
//        }
//
//        @Override
//        public String getFormat() {
//            return "JSON_FORMATTED";
//        }
//
//        @Override
//        public Object[] getParameters() {
//            return new Object[0];
//        }
//
//        @Override
//        public Throwable getThrowable() {
//            return null;
//        }
//
//        @Override
//        public void formatTo(StringBuilder buffer) {
//            String formattedMessage = getFormattedMessage();
//            buffer.append(formattedMessage);
//        }
//
//        @Override
//        public Object getValueFor(String key) {
//            return null;
//        }
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

    SlowLogLevel getLevel() {
        return level;
    }

}
