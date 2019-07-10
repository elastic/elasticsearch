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
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.metrics.MetricAggScriptPlugin.METRIC_SCRIPT_ENGINE;
import static org.elasticsearch.search.aggregations.metrics.MetricAggScriptPlugin.VALUE_FIELD_SCRIPT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

public class AvgIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MetricAggScriptPlugin.class);
    }

    /**
     * Make sure that a request using a script does not get cached and a request
     * not using a script does get cached.
     */
    public void testDontCacheScripts() throws Exception {
        assertAcked(prepareCreate("cache_test_idx").addMapping("type", "d", "type=long")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get());
        indexRandom(true, client().prepareIndex("cache_test_idx", "type", "1").setSource("s", 1),
                client().prepareIndex("cache_test_idx", "type", "2").setSource("s", 2));

        // Make sure we are starting with a clear cache
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(0L));

        // Test that a request using a script does not get cached
        SearchResponse r = client().prepareSearch("cache_test_idx").setSize(0)
                .addAggregation(avg("foo").field("d").script(
                    new Script(ScriptType.INLINE, METRIC_SCRIPT_ENGINE, VALUE_FIELD_SCRIPT, Collections.emptyMap()))).get();
        assertSearchResponse(r);

        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(0L));

        // To make sure that the cache is working test that a request not using
        // a script is cached
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(avg("foo").field("d")).get();
        assertSearchResponse(r);

        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(1L));
    }
}
