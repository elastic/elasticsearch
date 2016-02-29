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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.search.sort.SortOrder;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class UpdateByQueryBasicTests extends UpdateByQueryTestCase {
    public void testBasics() throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"),
                client().prepareIndex("test", "test", "2").setSource("foo", "a"),
                client().prepareIndex("test", "test", "3").setSource("foo", "b"),
                client().prepareIndex("test", "test", "4").setSource("foo", "c"));
        assertHitCount(client().prepareSearch("test").setTypes("test").setSize(0).get(), 4);
        assertEquals(1, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(1, client().prepareGet("test", "test", "4").get().getVersion());

        // Reindex all the docs
        assertThat(request().source("test").refresh(true).get(), responseMatcher().updated(4));
        assertEquals(2, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());

        // Now none of them
        assertThat(request().source("test").filter(termQuery("foo", "no_match")).refresh(true).get(), responseMatcher().updated(0));
        assertEquals(2, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());

        // Now half of them
        assertThat(request().source("test").filter(termQuery("foo", "a")).refresh(true).get(), responseMatcher().updated(2));
        assertEquals(3, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(3, client().prepareGet("test", "test", "2").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "3").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());

        // Limit with size
        UpdateByQueryRequestBuilder request = request().source("test").size(3).refresh(true);
        request.source().addSort("foo", SortOrder.ASC);
        assertThat(request.get(), responseMatcher().updated(3));
        // Only the first three documents are updated because of sort
        assertEquals(4, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(4, client().prepareGet("test", "test", "2").get().getVersion());
        assertEquals(3, client().prepareGet("test", "test", "3").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());
    }

    public void testRefreshIsFalseByDefault() throws Exception {
        refreshTestCase(null, false);
    }

    public void testRefreshFalseDoesntMakeVisible() throws Exception {
        refreshTestCase(false, false);
    }

    public void testRefreshTrueMakesVisible() throws Exception {
        refreshTestCase(true, true);
    }

    /**
     * Executes an update_by_query on an index with -1 refresh_interval and
     * checks that the documents are visible properly.
     */
    private void refreshTestCase(Boolean refresh, boolean visible) throws Exception {
        CreateIndexRequestBuilder create = client().admin().indices().prepareCreate("test").setSettings("refresh_interval", -1);
        create.addMapping("test", "{\"dynamic\": \"false\"}");
        assertAcked(create);
        ensureYellow();
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"),
                client().prepareIndex("test", "test", "2").setSource("foo", "a"),
                client().prepareIndex("test", "test", "3").setSource("foo", "b"),
                client().prepareIndex("test", "test", "4").setSource("foo", "c"));
        assertHitCount(client().prepareSearch("test").setQuery(matchQuery("foo", "a")).setSize(0).get(), 0);

        // Now make foo searchable
        assertAcked(client().admin().indices().preparePutMapping("test").setType("test")
                .setSource("{\"test\": {\"properties\":{\"foo\": {\"type\": \"string\"}}}}"));
        UpdateByQueryRequestBuilder update = request().source("test");
        if (refresh != null) {
            update.refresh(refresh);
        }
        assertThat(update.get(), responseMatcher().updated(4));

        assertHitCount(client().prepareSearch("test").setQuery(matchQuery("foo", "a")).setSize(0).get(), visible ? 2 : 0);
    }

}
