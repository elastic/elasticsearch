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

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class SearchSlowLogTests extends ESSingleNodeTestCase {
    @Override
    protected SearchContext createSearchContext(IndexService indexService) {
        BigArrays bigArrays = indexService.getBigArrays();
        ThreadPool threadPool = indexService.getThreadPool();
        return new TestSearchContext(threadPool, bigArrays, indexService) {
            final ShardSearchRequest request = new ShardSearchRequest() {
                private SearchSourceBuilder searchSourceBuilder;
                @Override
                public ShardId shardId() {
                    return new ShardId(indexService.index(), 0);
                }

                @Override
                public String[] types() {
                    return new String[0];
                }

                @Override
                public SearchSourceBuilder source() {
                    return searchSourceBuilder;
                }

                @Override
                public AliasFilter getAliasFilter() {
                    return new AliasFilter(QueryBuilders.matchAllQuery(), "foo");
                }

                @Override
                public void setAliasFilter(AliasFilter filter) {

                }

                @Override
                public void source(SearchSourceBuilder source) {
                    searchSourceBuilder = source;
                }

                @Override
                public int numberOfShards() {
                    return 0;
                }

                @Override
                public SearchType searchType() {
                    return null;
                }

                @Override
                public float indexBoost() {
                    return 1.0f;
                }

                @Override
                public long nowInMillis() {
                    return 0;
                }

                @Override
                public Boolean requestCache() {
                    return null;
                }

                @Override
                public Scroll scroll() {
                    return null;
                }

                @Override
                public void setProfile(boolean profile) {

                }

                @Override
                public boolean isProfile() {
                    return false;
                }

                @Override
                public BytesReference cacheKey() throws IOException {
                    return null;
                }

                @Override
                public Rewriteable getRewriteable() {
                    return null;
                }

                @Override
                public String getClusterAlias() {
                    return null;
                }
            };
            @Override
            public ShardSearchRequest request() {
                return request;
            }
        };
    }

    public void testSlowLogSearchContextPrinterToLog() throws IOException {
        IndexService index = createIndex("foo");
        SearchContext searchContext = createSearchContext(index);
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        searchContext.request().source(source);
        SearchSlowLog.SlowLogSearchContextPrinter p = new SearchSlowLog.SlowLogSearchContextPrinter(searchContext, 10);
        assertThat(p.toString(), startsWith("[foo][0]"));
        // Makes sure that output doesn't contain any new lines
        assertThat(p.toString(), not(containsString("\n")));
    }

    public void testLevelSetting() {
        SlowLogLevel level = randomFrom(SlowLogLevel.values());
        IndexMetaData metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), level)
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        SearchSlowLog log = new SearchSlowLog(settings);
        assertEquals(level, log.getLevel());
        level = randomFrom(SlowLogLevel.values());
        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), level).build()));
        assertEquals(level, log.getLevel());
        level = randomFrom(SlowLogLevel.values());
        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), level).build()));
        assertEquals(level, log.getLevel());


        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), level).build()));
        assertEquals(level, log.getLevel());

        settings.updateIndexMetaData(newIndexMeta("index", Settings.EMPTY));
        assertEquals(SlowLogLevel.TRACE, log.getLevel());

        metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build());
        settings = new IndexSettings(metaData, Settings.EMPTY);
        log = new SearchSlowLog(settings);
        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), "NOT A LEVEL").build()));
            fail();
        } catch (IllegalArgumentException ex) {
            final String expected = "illegal value can't update [index.search.slowlog.level] from [TRACE] to [NOT A LEVEL]";
            assertThat(ex, hasToString(containsString(expected)));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            final IllegalArgumentException cause = (IllegalArgumentException) ex.getCause();
            assertThat(cause, hasToString(containsString("No enum constant org.elasticsearch.index.SlowLogLevel.NOT A LEVEL")));
        }
        assertEquals(SlowLogLevel.TRACE, log.getLevel());
    }

    public void testSetQueryLevels() {
        IndexMetaData metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey(), "100ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(), "200ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(), "300ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), "400ms")
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        SearchSlowLog log = new SearchSlowLog(settings);
        assertEquals(TimeValue.timeValueMillis(100).nanos(), log.getQueryTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(200).nanos(), log.getQueryDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(300).nanos(), log.getQueryInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(400).nanos(), log.getQueryWarnThreshold());

        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey(), "120ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(), "220ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(), "320ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), "420ms").build()));


        assertEquals(TimeValue.timeValueMillis(120).nanos(), log.getQueryTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(220).nanos(), log.getQueryDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(320).nanos(), log.getQueryInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(420).nanos(), log.getQueryWarnThreshold());

        metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build());
        settings.updateIndexMetaData(metaData);
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryWarnThreshold());

        settings = new IndexSettings(metaData, Settings.EMPTY);
        log = new SearchSlowLog(settings);

        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryWarnThreshold());
        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey(), "NOT A TIME VALUE").build()));
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.query.trace");
        }

        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(), "NOT A TIME VALUE").build()));
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.query.debug");
        }

        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(), "NOT A TIME VALUE").build()));
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.query.info");
        }

        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), "NOT A TIME VALUE").build()));
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.query.warn");
        }
    }

    public void testSetFetchLevels() {
        IndexMetaData metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING.getKey(), "100ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING.getKey(), "200ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING.getKey(), "300ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING.getKey(), "400ms")
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        SearchSlowLog log = new SearchSlowLog(settings);
        assertEquals(TimeValue.timeValueMillis(100).nanos(), log.getFetchTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(200).nanos(), log.getFetchDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(300).nanos(), log.getFetchInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(400).nanos(), log.getFetchWarnThreshold());

        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING.getKey(), "120ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING.getKey(), "220ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING.getKey(), "320ms")
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING.getKey(), "420ms").build()));


        assertEquals(TimeValue.timeValueMillis(120).nanos(), log.getFetchTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(220).nanos(), log.getFetchDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(320).nanos(), log.getFetchInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(420).nanos(), log.getFetchWarnThreshold());

        metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build());
        settings.updateIndexMetaData(metaData);
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchWarnThreshold());

        settings = new IndexSettings(metaData, Settings.EMPTY);
        log = new SearchSlowLog(settings);

        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchWarnThreshold());
        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING.getKey(), "NOT A TIME VALUE").build()));
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.fetch.trace");
        }

        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING.getKey(), "NOT A TIME VALUE").build()));
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.fetch.debug");
        }

        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING.getKey(), "NOT A TIME VALUE").build()));
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.fetch.info");
        }

        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING.getKey(), "NOT A TIME VALUE").build()));
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.fetch.warn");
        }
    }

    private void assertTimeValueException(final IllegalArgumentException e, final String key) {
        final String expected = "illegal value can't update [" + key + "] from [-1] to [NOT A TIME VALUE]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String causeExpected =
                "failed to parse setting [" + key + "] with value [NOT A TIME VALUE] as a time value: unit is missing or unrecognized";
        assertThat(cause, hasToString(containsString(causeExpected)));
    }

    private IndexMetaData newIndexMeta(String name, Settings indexSettings) {
        Settings build = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(indexSettings)
            .build();
        IndexMetaData metaData = IndexMetaData.builder(name).settings(build).build();
        return metaData;
    }
}
