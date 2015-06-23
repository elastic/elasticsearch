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

package org.elasticsearch.index.indexing;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.settings.IndexSettingsService;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 */
public final class IndexingSlowLog {

    private boolean reformat;

    private long indexWarnThreshold;
    private long indexInfoThreshold;
    private long indexDebugThreshold;
    private long indexTraceThreshold;

    private String level;

    private final ESLogger indexLogger;
    private final ESLogger deleteLogger;

    private static final String INDEX_INDEXING_SLOWLOG_PREFIX = "index.indexing.slowlog";
    public static final String INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN = INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.warn";
    public static final String INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO = INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.info";
    public static final String INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG = INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.debug";
    public static final String INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE = INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.trace";
    public static final String INDEX_INDEXING_SLOWLOG_REFORMAT = INDEX_INDEXING_SLOWLOG_PREFIX +".reformat";
    public static final String INDEX_INDEXING_SLOWLOG_LEVEL = INDEX_INDEXING_SLOWLOG_PREFIX +".level";

    IndexingSlowLog(Settings indexSettings) {
        this.reformat = indexSettings.getAsBoolean(INDEX_INDEXING_SLOWLOG_REFORMAT, true);
        this.indexWarnThreshold = indexSettings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN, TimeValue.timeValueNanos(-1)).nanos();
        this.indexInfoThreshold = indexSettings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO, TimeValue.timeValueNanos(-1)).nanos();
        this.indexDebugThreshold = indexSettings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG, TimeValue.timeValueNanos(-1)).nanos();
        this.indexTraceThreshold = indexSettings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE, TimeValue.timeValueNanos(-1)).nanos();

        this.level = indexSettings.get(INDEX_INDEXING_SLOWLOG_LEVEL, "TRACE").toUpperCase(Locale.ROOT);

        this.indexLogger = Loggers.getLogger(INDEX_INDEXING_SLOWLOG_PREFIX +".index");
        this.deleteLogger = Loggers.getLogger(INDEX_INDEXING_SLOWLOG_PREFIX +".delete");

        indexLogger.setLevel(level);
        deleteLogger.setLevel(level);
    }

    synchronized void onRefreshSettings(Settings settings) {
        long indexWarnThreshold = settings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN, TimeValue.timeValueNanos(this.indexWarnThreshold)).nanos();
        if (indexWarnThreshold != this.indexWarnThreshold) {
            this.indexWarnThreshold = indexWarnThreshold;
        }
        long indexInfoThreshold = settings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO, TimeValue.timeValueNanos(this.indexInfoThreshold)).nanos();
        if (indexInfoThreshold != this.indexInfoThreshold) {
            this.indexInfoThreshold = indexInfoThreshold;
        }
        long indexDebugThreshold = settings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG, TimeValue.timeValueNanos(this.indexDebugThreshold)).nanos();
        if (indexDebugThreshold != this.indexDebugThreshold) {
            this.indexDebugThreshold = indexDebugThreshold;
        }
        long indexTraceThreshold = settings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE, TimeValue.timeValueNanos(this.indexTraceThreshold)).nanos();
        if (indexTraceThreshold != this.indexTraceThreshold) {
            this.indexTraceThreshold = indexTraceThreshold;
        }

        String level = settings.get(INDEX_INDEXING_SLOWLOG_LEVEL, this.level);
        if (!level.equals(this.level)) {
            this.indexLogger.setLevel(level.toUpperCase(Locale.ROOT));
            this.deleteLogger.setLevel(level.toUpperCase(Locale.ROOT));
            this.level = level;
        }

        boolean reformat = settings.getAsBoolean(INDEX_INDEXING_SLOWLOG_REFORMAT, this.reformat);
        if (reformat != this.reformat) {
            this.reformat = reformat;
        }
    }

    void postIndex(Engine.Index index, long tookInNanos) {
        postIndexing(index.parsedDoc(), tookInNanos);
    }

    void postCreate(Engine.Create create, long tookInNanos) {
        postIndexing(create.parsedDoc(), tookInNanos);
    }

    private void postIndexing(ParsedDocument doc, long tookInNanos) {
        if (indexWarnThreshold >= 0 && tookInNanos > indexWarnThreshold) {
            indexLogger.warn("{}", new SlowLogParsedDocumentPrinter(doc, tookInNanos, reformat));
        } else if (indexInfoThreshold >= 0 && tookInNanos > indexInfoThreshold) {
            indexLogger.info("{}", new SlowLogParsedDocumentPrinter(doc, tookInNanos, reformat));
        } else if (indexDebugThreshold >= 0 && tookInNanos > indexDebugThreshold) {
            indexLogger.debug("{}", new SlowLogParsedDocumentPrinter(doc, tookInNanos, reformat));
        } else if (indexTraceThreshold >= 0 && tookInNanos > indexTraceThreshold) {
            indexLogger.trace("{}", new SlowLogParsedDocumentPrinter(doc, tookInNanos, reformat));
        }
    }

    final static class SlowLogParsedDocumentPrinter {
        private final ParsedDocument doc;
        private final long tookInNanos;
        private final boolean reformat;

        SlowLogParsedDocumentPrinter(ParsedDocument doc, long tookInNanos, boolean reformat) {
            this.doc = doc;
            this.tookInNanos = tookInNanos;
            this.reformat = reformat;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("took[").append(TimeValue.timeValueNanos(tookInNanos)).append("], took_millis[").append(TimeUnit.NANOSECONDS.toMillis(tookInNanos)).append("], ");
            sb.append("type[").append(doc.type()).append("], ");
            sb.append("id[").append(doc.id()).append("], ");
            if (doc.routing() == null) {
                sb.append("routing[], ");
            } else {
                sb.append("routing[").append(doc.routing()).append("], ");
            }
            if (doc.source() != null && doc.source().length() > 0) {
                try {
                    sb.append("source[").append(XContentHelper.convertToJson(doc.source(), reformat)).append("]");
                } catch (IOException e) {
                    sb.append("source[_failed_to_convert_]");
                }
            } else {
                sb.append("source[]");
            }
            return sb.toString();
        }
    }
}