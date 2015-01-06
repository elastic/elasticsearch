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

package org.elasticsearch.search.timeout;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import static org.elasticsearch.index.query.FilterBuilders.scriptFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope=ElasticsearchIntegrationTest.Scope.SUITE)
public class SearchTimeoutTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                .put(GroovyScriptEngineService.GROOVY_SCRIPT_SANDBOX_ENABLED, false)
                .put(ThreadPool.THREADPOOL_GROUP + ThreadPool.Names.ESTIMATED_TIME_INTERVAL, 100).build();
    }

    @Test
    public void simpleTimeoutTest() throws Exception {
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test").setTimeout("200ms")
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("Thread.sleep(500); return true;"))).execute().actionGet();
        assertThat(searchResponse.isTimedOut(), equalTo(true));
    }

    @Test
    public void invalidTimeoutTest() throws Exception {
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefresh(true).execute().actionGet();

        ElasticsearchException expected = null;
        try {
            // Timeout parameters must be at least double the resolution of the
            // interval used by the estimatedTimeCounter inside a node.
            client().prepareSearch("test").setTimeout("10ms")
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("Thread.sleep(500); return true;")))
                .execute().actionGet();
        } catch (ElasticsearchException e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains("SearchParseException"));
    }

}
