/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.search.SearchTimeoutIT.ScriptedTimeoutPlugin.SCRIPT_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
@ESIntegTestCase.SuiteScopeTestCase
public class SearchTimeoutIT extends ESIntegTestCase {

    private static final AtomicInteger scriptExecutions = new AtomicInteger(0);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ScriptedTimeoutPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        super.setupSuiteScopeCluster();
        indexRandom(true, "test", randomIntBetween(20, 50));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        scriptExecutions.set(0);
    }

    @SuppressForbidden(reason = "just a test")
    @Repeat(iterations = 100)
    public void testTopHitsTimeout() {
        SearchRequestBuilder searchRequestBuilder = prepareSearch("test").setTimeout(new TimeValue(100, TimeUnit.MILLISECONDS))
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        ElasticsearchAssertions.assertResponse(searchRequestBuilder, searchResponse -> {
            assertThat(searchResponse.isTimedOut(), equalTo(true));
            assertEquals(0, searchResponse.getShardFailures().length);
            assertEquals(0, searchResponse.getFailedShards());
            assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
            assertEquals(searchResponse.getSuccessfulShards(), searchResponse.getTotalShards());
            assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
            assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
        });
    }

    @SuppressForbidden(reason = "just a test")
    @Repeat(iterations = 100)
    public void testAggsTimeout() {
        SearchRequestBuilder searchRequestBuilder = prepareSearch("test").setTimeout(new TimeValue(100, TimeUnit.MILLISECONDS))
            .setSize(0)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
            .addAggregation(new TermsAggregationBuilder("terms").field("field.keyword"));
        ElasticsearchAssertions.assertResponse(searchRequestBuilder, searchResponse -> {
            assertThat(searchResponse.isTimedOut(), equalTo(true));
            assertEquals(0, searchResponse.getShardFailures().length);
            assertEquals(0, searchResponse.getFailedShards());
            assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
            assertEquals(searchResponse.getSuccessfulShards(), searchResponse.getTotalShards());
            assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
            assertEquals(0, searchResponse.getHits().getHits().length);
            StringTerms terms = searchResponse.getAggregations().get("terms");
            assertEquals(1, terms.getBuckets().size());
            StringTerms.Bucket bucket = terms.getBuckets().get(0);
            assertEquals("value", bucket.getKeyAsString());
            assertThat(bucket.getDocCount(), greaterThan(0L));
        });
    }

    public void testPartialResultsIntolerantTimeout() {
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
                .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .setAllowPartialSearchResults(false) // this line causes timeouts to report failures
        );
        assertTrue(ex.toString().contains("Time exceeded"));
        assertEquals(504, ex.status().getStatus());
    }

    public static class ScriptedTimeoutPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_timeout";

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                // sleep only once per test, but only after executing the script once without sleeping.
                // This ensures that one document is always returned before the timeout happens.
                // Also, don't sleep any further to avoid slowing down the test excessively.
                // A timeout on a specific slice of a single shard is enough.
                if (scriptExecutions.getAndIncrement() == 1) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new ThreadInterruptedException(e);
                    }
                }
                return true;
            });
        }
    }
}
