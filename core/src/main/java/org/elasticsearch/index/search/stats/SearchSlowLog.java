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

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 */
public final class SearchSlowLog{

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

    private static final String INDEX_SEARCH_SLOWLOG_PREFIX = "index.search.slowlog";
    public static final String INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN = INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.warn";
    public static final String INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO = INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.info";
    public static final String INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG = INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.debug";
    public static final String INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE = INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.trace";
    public static final String INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN = INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.warn";
    public static final String INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO = INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.info";
    public static final String INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG = INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.debug";
    public static final String INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE = INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.trace";
    public static final String INDEX_SEARCH_SLOWLOG_REFORMAT = INDEX_SEARCH_SLOWLOG_PREFIX + ".reformat";
    public static final String INDEX_SEARCH_SLOWLOG_LEVEL = INDEX_SEARCH_SLOWLOG_PREFIX + ".level";

    SearchSlowLog(Settings indexSettings) {

        this.reformat = indexSettings.getAsBoolean(INDEX_SEARCH_SLOWLOG_REFORMAT, true);

        this.queryWarnThreshold = indexSettings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN, TimeValue.timeValueNanos(-1)).nanos();
        this.queryInfoThreshold = indexSettings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO, TimeValue.timeValueNanos(-1)).nanos();
        this.queryDebugThreshold = indexSettings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG, TimeValue.timeValueNanos(-1)).nanos();
        this.queryTraceThreshold = indexSettings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE, TimeValue.timeValueNanos(-1)).nanos();

        this.fetchWarnThreshold = indexSettings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN, TimeValue.timeValueNanos(-1)).nanos();
        this.fetchInfoThreshold = indexSettings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO, TimeValue.timeValueNanos(-1)).nanos();
        this.fetchDebugThreshold = indexSettings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG, TimeValue.timeValueNanos(-1)).nanos();
        this.fetchTraceThreshold = indexSettings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE, TimeValue.timeValueNanos(-1)).nanos();

        this.level = indexSettings.get(INDEX_SEARCH_SLOWLOG_LEVEL, "TRACE").toUpperCase(Locale.ROOT);

        this.queryLogger = Loggers.getLogger(INDEX_SEARCH_SLOWLOG_PREFIX + ".query");
        this.fetchLogger = Loggers.getLogger(INDEX_SEARCH_SLOWLOG_PREFIX + ".fetch");

        queryLogger.setLevel(level);
        fetchLogger.setLevel(level);
    }

    void onQueryPhase(SearchContext context, long tookInNanos) {
        if (queryWarnThreshold >= 0 && tookInNanos > queryWarnThreshold) {
            queryLogger.warn("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (queryInfoThreshold >= 0 && tookInNanos > queryInfoThreshold) {
            queryLogger.info("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (queryDebugThreshold >= 0 && tookInNanos > queryDebugThreshold) {
            queryLogger.debug("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (queryTraceThreshold >= 0 && tookInNanos > queryTraceThreshold) {
            queryLogger.trace("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        }
    }

    void onFetchPhase(SearchContext context, long tookInNanos) {
        if (fetchWarnThreshold >= 0 && tookInNanos > fetchWarnThreshold) {
            fetchLogger.warn("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (fetchInfoThreshold >= 0 && tookInNanos > fetchInfoThreshold) {
            fetchLogger.info("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (fetchDebugThreshold >= 0 && tookInNanos > fetchDebugThreshold) {
            fetchLogger.debug("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        } else if (fetchTraceThreshold >= 0 && tookInNanos > fetchTraceThreshold) {
            fetchLogger.trace("{}", new SlowLogSearchContextPrinter(context, tookInNanos, reformat));
        }
    }

    synchronized void onRefreshSettings(Settings settings) {
        long queryWarnThreshold = settings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN, TimeValue.timeValueNanos(this.queryWarnThreshold)).nanos();
        if (queryWarnThreshold != this.queryWarnThreshold) {
            this.queryWarnThreshold = queryWarnThreshold;
        }
        long queryInfoThreshold = settings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO, TimeValue.timeValueNanos(this.queryInfoThreshold)).nanos();
        if (queryInfoThreshold != this.queryInfoThreshold) {
            this.queryInfoThreshold = queryInfoThreshold;
        }
        long queryDebugThreshold = settings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG, TimeValue.timeValueNanos(this.queryDebugThreshold)).nanos();
        if (queryDebugThreshold != this.queryDebugThreshold) {
            this.queryDebugThreshold = queryDebugThreshold;
        }
        long queryTraceThreshold = settings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE, TimeValue.timeValueNanos(this.queryTraceThreshold)).nanos();
        if (queryTraceThreshold != this.queryTraceThreshold) {
            this.queryTraceThreshold = queryTraceThreshold;
        }

        long fetchWarnThreshold = settings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN, TimeValue.timeValueNanos(this.fetchWarnThreshold)).nanos();
        if (fetchWarnThreshold != this.fetchWarnThreshold) {
            this.fetchWarnThreshold = fetchWarnThreshold;
        }
        long fetchInfoThreshold = settings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO, TimeValue.timeValueNanos(this.fetchInfoThreshold)).nanos();
        if (fetchInfoThreshold != this.fetchInfoThreshold) {
            this.fetchInfoThreshold = fetchInfoThreshold;
        }
        long fetchDebugThreshold = settings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG, TimeValue.timeValueNanos(this.fetchDebugThreshold)).nanos();
        if (fetchDebugThreshold != this.fetchDebugThreshold) {
            this.fetchDebugThreshold = fetchDebugThreshold;
        }
        long fetchTraceThreshold = settings.getAsTime(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE, TimeValue.timeValueNanos(this.fetchTraceThreshold)).nanos();
        if (fetchTraceThreshold != this.fetchTraceThreshold) {
            this.fetchTraceThreshold = fetchTraceThreshold;
        }

        String level = settings.get(INDEX_SEARCH_SLOWLOG_LEVEL, this.level);
        if (!level.equals(this.level)) {
            this.queryLogger.setLevel(level.toUpperCase(Locale.ROOT));
            this.fetchLogger.setLevel(level.toUpperCase(Locale.ROOT));
            this.level = level;
        }

        boolean reformat = settings.getAsBoolean(INDEX_SEARCH_SLOWLOG_REFORMAT, this.reformat);
        if (reformat != this.reformat) {
            this.reformat = reformat;
        }
    }

    private static class SlowLogSearchContextPrinter {
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
            if (context.types() == null) {
                sb.append("types[], ");
            } else {
                sb.append("types[");
                Strings.arrayToDelimitedString(context.types(), ",", sb);
                sb.append("], ");
            }
            if (context.groupStats() == null) {
                sb.append("stats[], ");
            } else {
                sb.append("stats[");
                Strings.collectionToDelimitedString(context.groupStats(), ",", "", "", sb);
                sb.append("], ");
            }
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
