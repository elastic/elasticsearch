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

package org.elasticsearch.index.indexing.slowlog;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 */
public class ShardSlowLogIndexingService extends AbstractIndexShardComponent {

    private boolean reformat;

    private long indexWarnThreshold;
    private long indexInfoThreshold;
    private long indexDebugThreshold;
    private long indexTraceThreshold;

    private String level;

    private final ESLogger indexLogger;
    private final ESLogger deleteLogger;

    public static final String INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN = "index.indexing.slowlog.threshold.index.warn";
    public static final String INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO = "index.indexing.slowlog.threshold.index.info";
    public static final String INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG = "index.indexing.slowlog.threshold.index.debug";
    public static final String INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE = "index.indexing.slowlog.threshold.index.trace";
    public static final String INDEX_INDEXING_SLOWLOG_REFORMAT = "index.indexing.slowlog.reformat";
    public static final String INDEX_INDEXING_SLOWLOG_LEVEL = "index.indexing.slowlog.level";

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public synchronized void onRefreshSettings(Settings settings) {
            long indexWarnThreshold = settings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN, TimeValue.timeValueNanos(ShardSlowLogIndexingService.this.indexWarnThreshold)).nanos();
            if (indexWarnThreshold != ShardSlowLogIndexingService.this.indexWarnThreshold) {
                ShardSlowLogIndexingService.this.indexWarnThreshold = indexWarnThreshold;
            }
            long indexInfoThreshold = settings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO, TimeValue.timeValueNanos(ShardSlowLogIndexingService.this.indexInfoThreshold)).nanos();
            if (indexInfoThreshold != ShardSlowLogIndexingService.this.indexInfoThreshold) {
                ShardSlowLogIndexingService.this.indexInfoThreshold = indexInfoThreshold;
            }
            long indexDebugThreshold = settings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG, TimeValue.timeValueNanos(ShardSlowLogIndexingService.this.indexDebugThreshold)).nanos();
            if (indexDebugThreshold != ShardSlowLogIndexingService.this.indexDebugThreshold) {
                ShardSlowLogIndexingService.this.indexDebugThreshold = indexDebugThreshold;
            }
            long indexTraceThreshold = settings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE, TimeValue.timeValueNanos(ShardSlowLogIndexingService.this.indexTraceThreshold)).nanos();
            if (indexTraceThreshold != ShardSlowLogIndexingService.this.indexTraceThreshold) {
                ShardSlowLogIndexingService.this.indexTraceThreshold = indexTraceThreshold;
            }

            String level = settings.get(INDEX_INDEXING_SLOWLOG_LEVEL, ShardSlowLogIndexingService.this.level);
            if (!level.equals(ShardSlowLogIndexingService.this.level)) {
                ShardSlowLogIndexingService.this.indexLogger.setLevel(level.toUpperCase(Locale.ROOT));
                ShardSlowLogIndexingService.this.deleteLogger.setLevel(level.toUpperCase(Locale.ROOT));
                ShardSlowLogIndexingService.this.level = level;
            }

            boolean reformat = settings.getAsBoolean(INDEX_INDEXING_SLOWLOG_REFORMAT, ShardSlowLogIndexingService.this.reformat);
            if (reformat != ShardSlowLogIndexingService.this.reformat) {
                ShardSlowLogIndexingService.this.reformat = reformat;
            }
        }
    }

    @Inject
    public ShardSlowLogIndexingService(ShardId shardId, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService) {
        super(shardId, indexSettings);

        this.reformat = indexSettings.getAsBoolean(INDEX_INDEXING_SLOWLOG_REFORMAT, true);

        this.indexWarnThreshold = indexSettings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN, TimeValue.timeValueNanos(-1)).nanos();
        this.indexInfoThreshold = indexSettings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO, TimeValue.timeValueNanos(-1)).nanos();
        this.indexDebugThreshold = indexSettings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG, TimeValue.timeValueNanos(-1)).nanos();
        this.indexTraceThreshold = indexSettings.getAsTime(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE, TimeValue.timeValueNanos(-1)).nanos();

        this.level = indexSettings.get(INDEX_INDEXING_SLOWLOG_LEVEL, "TRACE").toUpperCase(Locale.ROOT);

        this.indexLogger = Loggers.getLogger(logger, ".index");
        this.deleteLogger = Loggers.getLogger(logger, ".delete");

        indexLogger.setLevel(level);
        deleteLogger.setLevel(level);

        indexSettingsService.addListener(new ApplySettings());
    }

    public void postIndex(Engine.Index index, long tookInNanos) {
        postIndexing(index.parsedDoc(), tookInNanos);
    }

    public void postCreate(Engine.Create create, long tookInNanos) {
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

    public static class SlowLogParsedDocumentPrinter {
        private final ParsedDocument doc;
        private final long tookInNanos;
        private final boolean reformat;

        public SlowLogParsedDocumentPrinter(ParsedDocument doc, long tookInNanos, boolean reformat) {
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