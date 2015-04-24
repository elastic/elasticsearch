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


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SearchServiceTests extends ElasticsearchSingleNodeTest {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testClearOnClose() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefresh(true).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearOnStop() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefresh(true).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doStop();
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearIndexDelete() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefresh(true).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        assertAcked(client().admin().indices().prepareDelete("index"));
        assertEquals(0, service.getActiveContexts());
    }
}
