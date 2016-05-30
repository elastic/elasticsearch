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

import org.elasticsearch.search.sort.SortOrder;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class UpdateByQueryBasicTests extends ReindexTestCase {
    public void testBasics() throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"),
                client().prepareIndex("test", "test", "2").setSource("foo", "a"),
                client().prepareIndex("test", "test", "3").setSource("foo", "b"),
                client().prepareIndex("test", "test", "4").setSource("foo", "c"));
        assertHitCount(client().prepareSearch("test").setTypes("test").setSize(0).get(), 4);
        assertEquals(1, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(1, client().prepareGet("test", "test", "4").get().getVersion());

        // Reindex all the docs
        assertThat(updateByQuery().source("test").refresh(true).get(), matcher().updated(4));
        assertEquals(2, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());

        // Now none of them
        assertThat(updateByQuery().source("test").filter(termQuery("foo", "no_match")).refresh(true).get(), matcher().updated(0));
        assertEquals(2, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());

        // Now half of them
        assertThat(updateByQuery().source("test").filter(termQuery("foo", "a")).refresh(true).get(), matcher().updated(2));
        assertEquals(3, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(3, client().prepareGet("test", "test", "2").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "3").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());

        // Limit with size
        UpdateByQueryRequestBuilder request = updateByQuery().source("test").size(3).refresh(true);
        request.source().addSort("foo.keyword", SortOrder.ASC);
        assertThat(request.get(), matcher().updated(3));
        // Only the first three documents are updated because of sort
        assertEquals(4, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(4, client().prepareGet("test", "test", "2").get().getVersion());
        assertEquals(3, client().prepareGet("test", "test", "3").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());
    }
}
