/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.search.slowlog;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 */
public class ShardSlowLogSearchService extends AbstractIndexShardComponent {

    private boolean reformat;

    private long queryWarnThreshold;
    private long queryInfoThreshold;
    private long queryDebugThreshold;
    private long queryTraceThreshold;

    private long fetchWarnThreshold;
    private long fetchInfoThreshold;
    private long fetchDebugThreshold;
    private long fetchTraceThreshold;

    private String level;

    private final ESLogger queryLogger;
    private final ESLogger fetchLogger;

    static {
        IndexMetaData.addDynamicSettings(
                "index.search.slowlog.threshold.query.warn",
                "index.search.slowlog.threshold.query.info",
                "index.search.slowlog.threshold.query.debug",
                "index.search.slowlog.threshold.query.trace",
                "index.search.slowlog.threshold.fetch.warn",
                "index.search.slowlog.threshold.fetch.info",
                "index.search.slowlog.threshold.fetch.debug",
                "index.search.slowlog.threshold.fetch.trace",
                "index.search.slowlog.reformat",
                "index.search.slowlog.level"
        );
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public synchronized void onRefreshSettings(Settings settings) {
            long queryWarnThreshold = settings.getAsTime("index.search.slowlog.threshold.query.warn", TimeValue.timeValueNanos(ShardSlowLogSearchService.this.queryWarnThreshold)).nanos();
            if (queryWarnThreshold != ShardSlowLogSearchService.this.queryWarnThreshold) {
                ShardSlowLogSearchService.this.queryWarnThreshold = queryWarnThreshold;
            }
            long queryInfoThreshold = settings.getAsTime("index.search.slowlog.threshold.query.info", TimeValue.timeValueNanos(ShardSlowLogSearchService.this.queryInfoThreshold)).nanos();
            if (queryInfoThreshold != ShardSlowLogSearchService.this.queryInfoThreshold) {
                ShardSlowLogSearchService.this.queryInfoThreshold = queryInfoThreshold;
            }
            long queryDebugThreshold = settings.getAsTime("index.search.slowlog.threshold.query.debug", TimeValue.timeValueNanos(ShardSlowLogSearchService.this.queryDebugThreshold)).nanos();
            if (queryDebugThreshold != ShardSlowLogSearchService.this.queryDebugThreshold) {
                ShardSlowLogSearchService.this.queryDebugThreshold = queryDebugThreshold;
            }
            long queryTraceThreshold = settings.getAsTime("index.search.slowlog.threshold.query.trace", TimeValue.timeValueNanos(ShardSlowLogSearchService.this.queryTraceThreshold)).nanos();
            if (queryTraceThreshold != ShardSlowLogSearchService.this.queryTraceThreshold) {
                ShardSlowLogSearchService.this.queryTraceThreshold = queryTraceThreshold;
            }

            long fetchWarnThreshold = settings.getAsTime("index.search.slowlog.threshold.fetch.warn", TimeValue.timeValueNanos(ShardSlowLogSearchService.this.fetchWarnThreshold)).nanos();
            if (fetchWarnThreshold != ShardSlowLogSearchService.this.fetchWarnThreshold) {
                ShardSlowLogSearchService.this.fetchWarnThreshold = fetchWarnThreshold;
            }
            long fetchInfoThreshold = settings.getAsTime("index.search.slowlog.threshold.fetch.info", TimeValue.timeValueNanos(ShardSlowLogSearchService.this.fetchInfoThreshold)).nanos();
            if (fetchInfoThreshold != ShardSlowLogSearchService.this.fetchInfoThreshold) {
                ShardSlowLogSearchService.this.fetchInfoThreshold = fetchInfoThreshold;
            }
            long fetchDebugThreshold = settings.getAsTime("index.search.slowlog.threshold.fetch.debug", TimeValue.timeValueNanos(ShardSlowLogSearchService.this.fetchDebugThreshold)).nanos();
            if (fetchDebugThreshold != ShardSlowLogSearchService.this.fetchDebugThreshold) {
                ShardSlowLogSearchService.this.fetchDebugThreshold = fetchDebugThreshold;
            }
            long fetchTraceThreshold = settings.getAsTime("index.search.slowlog.threshold.fetch.trace", TimeValue.timeValueNanos(ShardSlowLogSearchService.this.fetchTraceThreshold)).nanos();
            if (fetchTraceThreshold != ShardSlowLogSearchService.this.fetchTraceThreshold) {
                ShardSlowLogSearchService.this.fetchTraceThreshold = fetchTraceThreshold;
            }

            String level = settings.get("index.search.slowlog.level", ShardSlowLogSearchService.this.level);
            if (!level.equals(ShardSlowLogSearchService.this.level)) {
                ShardSlowLogSearchService.this.queryLogger.setLevel(level.toUpperCase());
                ShardSlowLogSearchService.this.fetchLogger.setLevel(level.toUpperCase());
                ShardSlowLogSearchService.this.level = level;
            }

            boolean reformat = settings.getAsBoolean("index.search.slowlog.reformat", ShardSlowLogSearchService.this.reformat);
            if (reformat != ShardSlowLogSearchService.this.reformat) {
                ShardSlowLogSearchService.this.reformat = reformat;
            }
        }
    }

    @Inject
    public ShardSlowLogSearchService(ShardId shardId, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService) {
        super(shardId, indexSettings);

        this.reformat = componentSettings.getAsBoolean("reformat", true);

        this.queryWarnThreshold = componentSettings.getAsTime("threshold.query.warn", TimeValue.timeValueNanos(-1)).nanos();
        this.queryInfoThreshold = componentSettings.getAsTime("threshold.query.info", TimeValue.timeValueNanos(-1)).nanos();
        this.queryDebugThreshold = componentSettings.getAsTime("threshold.query.debug", TimeValue.timeValueNanos(-1)).nanos();
        this.queryTraceThreshold = componentSettings.getAsTime("threshold.query.trace", TimeValue.timeValueNanos(-1)).nanos();

        this.fetchWarnThreshold = componentSettings.getAsTime("threshold.fetch.warn", TimeValue.timeValueNanos(-1)).nanos();
        this.fetchInfoThreshold = componentSettings.getAsTime("threshold.fetch.info", TimeValue.timeValueNanos(-1)).nanos();
        this.fetchDebugThreshold = componentSettings.getAsTime("threshold.fetch.debug", TimeValue.timeValueNanos(-1)).nanos();
        this.fetchTraceThreshold = componentSettings.getAsTime("threshold.fetch.trace", TimeValue.timeValueNanos(-1)).nanos();

        this.level = componentSettings.get("level", "TRACE").toUpperCase();

        this.queryLogger = Loggers.getLogger(logger, ".query");
        this.fetchLogger = Loggers.getLogger(logger, ".fetch");

        queryLogger.setLevel(level);
        fetchLogger.setLevel(level);

        indexSettingsService.addListener(new ApplySettings());
    }

    public void onQueryPhase(SearchContext context, long tookInNanos) {
        if (queryWarnThreshold >= 0 && tookInNanos > queryWarnThreshold && queryLogger.isWarnEnabled()) {
            queryLogger.warn("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (queryInfoThreshold >= 0 && tookInNanos > queryInfoThreshold && queryLogger.isInfoEnabled()) {
            queryLogger.info("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (queryDebugThreshold >= 0 && tookInNanos > queryDebugThreshold && queryLogger.isDebugEnabled()) {
            queryLogger.debug("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (queryTraceThreshold >= 0 && tookInNanos > queryTraceThreshold && queryLogger.isTraceEnabled()) {
            queryLogger.trace("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        }
    }

    public void onFetchPhase(SearchContext context, long tookInNanos) {
        if (fetchWarnThreshold >= 0 && tookInNanos > fetchWarnThreshold && fetchLogger.isWarnEnabled()) {
            fetchLogger.warn("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (fetchInfoThreshold >= 0 && tookInNanos > fetchInfoThreshold && fetchLogger.isInfoEnabled()) {
            fetchLogger.info("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (fetchDebugThreshold >= 0 && tookInNanos > fetchDebugThreshold && fetchLogger.isDebugEnabled()) {
            fetchLogger.debug("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (fetchTraceThreshold >= 0 && tookInNanos > fetchTraceThreshold && fetchLogger.isTraceEnabled()) {
            fetchLogger.trace("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        }
    }

    public static class SlowLogSearchContextPrinter {
        private final SearchContext context;
        private final long tookInNanos;
        private final boolean reformat;

        public SlowLogSearchContextPrinter(SearchContext context, long tookInNanos, boolean reformat) {
            this.context = context;
            this.tookInNanos = tookInNanos;
            this.reformat = reformat;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("took[").append(TimeValue.timeValueNanos(tookInNanos)).append("], took_millis[").append(TimeUnit.NANOSECONDS.toMillis(tookInNanos)).append("], ");
            sb.append("search_type[").append(context.searchType()).append("], total_shards[").append(context.numberOfShards()).append("], ");
            if (context.request().source() != null && context.request().source().length() > 0) {
                try {
                    sb.append("source[").append(XContentHelper.convertToJson(context.request().source(), reformat)).append("], ");
                } catch (IOException e) {
                    sb.append("source[_failed_to_convert_], ");
                }
            } else {
                sb.append("source[], ");
            }
            if (context.request().extraSource() != null && context.request().extraSource().length() > 0) {
                try {
                    sb.append("extra_source[").append(XContentHelper.convertToJson(context.request().extraSource(), reformat)).append("], ");
                } catch (IOException e) {
                    sb.append("extra_source[_failed_to_convert_], ");
                }
            } else {
                sb.append("extra_source[], ");
            }
            return sb.toString();
        }
    }
}
