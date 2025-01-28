/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.search.SearchTimeoutIT.ScriptedTimeoutPlugin.SCRIPT_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SearchTimeoutIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ScriptedTimeoutPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
    }

    private void indexDocs() {
        for (int i = 0; i < 32; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value").get();
        }
        refresh("test");
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/98369")
    public void testTopHitsTimeout() {
        indexDocs();
        SearchResponse searchResponse = prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
            .get();
        assertThat(searchResponse.isTimedOut(), equalTo(true));
        assertEquals(0, searchResponse.getShardFailures().length);
        assertEquals(0, searchResponse.getFailedShards());
        assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
        assertEquals(searchResponse.getSuccessfulShards(), searchResponse.getTotalShards());
        assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
        assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/98053")
    public void testAggsTimeout() {
        indexDocs();
        SearchResponse searchResponse = prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
            .setSize(0)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
            .addAggregation(new TermsAggregationBuilder("terms").field("field.keyword"))
            .get();
        assertThat(searchResponse.isTimedOut(), equalTo(true));
        assertEquals(0, searchResponse.getShardFailures().length);
        assertEquals(0, searchResponse.getFailedShards());
        assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
        assertEquals(searchResponse.getSuccessfulShards(), searchResponse.getTotalShards());
        assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
        assertEquals(searchResponse.getHits().getHits().length, 0);
        StringTerms terms = searchResponse.getAggregations().get("terms");
        assertEquals(1, terms.getBuckets().size());
        StringTerms.Bucket bucket = terms.getBuckets().get(0);
        assertEquals("value", bucket.getKeyAsString());
        assertThat(bucket.getDocCount(), greaterThan(0L));
    }

    public void testPartialResultsIntolerantTimeout() throws Exception {
        prepareIndex("test").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
                .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .setAllowPartialSearchResults(false) // this line causes timeouts to report failures
        );
        assertTrue(ex.toString().contains("Time exceeded"));
    }

    public static class ScriptedTimeoutPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_timeout";

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}
