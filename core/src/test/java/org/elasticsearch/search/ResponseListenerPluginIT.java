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

package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static java.util.Collections.singletonList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThan;

public class ResponseListenerPluginIT extends ESIntegTestCase {

    static AtomicBoolean hookWasFired = new AtomicBoolean(false);

    public static class FooResponseListenerPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<BiConsumer<SearchRequest, SearchResponse>> getSearchResponseListeners() {
            return singletonList((searchRequest, response) -> {
                assertThat(response.getTookInMillis(), greaterThan(0L));
                boolean alreadyFired = hookWasFired.getAndSet(true);
                assertFalse(alreadyFired);
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(FooResponseListenerPlugin.class);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createIndex("test");
        ensureGreen();
        client().prepareIndex("index", "type", "1").setSource("field", "value").get();
        refresh();
    }

    public void testSearchResponseHook() {
        assertHitCount(client().prepareSearch("index").setQuery(QueryBuilders.matchAllQuery()).get(), 1L);
        boolean alreadyFired = hookWasFired.get();
        assertTrue(alreadyFired);
    }
}
