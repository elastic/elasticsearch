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

package org.elasticsearch.plugin.indexbysearch;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.index.IndexRequestBuilder;

public class IndexBySearchBasicTests extends IndexBySearchTestCase {
    public void testBasicsIntoExistingIndex() throws Exception {
        basics(true);
    }

    public void testBasicsIntoNewIndex() throws Exception {
        basics(false);
    }

    private void basics(boolean destinationIndexIsSourceIndex) throws Exception {
        indexRandom(true, client().prepareIndex("test", "source", "1").setSource("foo", "a"),
                client().prepareIndex("test", "source", "2").setSource("foo", "a"),
                client().prepareIndex("test", "source", "3").setSource("foo", "b"),
                client().prepareIndex("test", "source", "4").setSource("foo", "c"));
        assertHitCount(client().prepareSearch("test").setTypes("source").setSize(0).get(), 4);

        String destinationIndex = destinationIndexIsSourceIndex ? "test" : "not_test";

        // Copy all the docs
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.index().setIndex(destinationIndex).setType("dest_all");
        assertThat(copy.get(), responseMatcher().created(4));
        refresh();
        assertHitCount(client().prepareSearch(destinationIndex).setTypes("dest_all").setSize(0).get(), 4);

        // Now none of them
        copy = newIndexBySearch();
        copy.search().setQuery(termQuery("foo", "no_match"));
        copy.index().setIndex(destinationIndex).setType("dest_none");
        assertThat(copy.get(), responseMatcher().created(0));
        refresh();
        assertHitCount(client().prepareSearch(destinationIndex).setTypes("dest_none").setSize(0).get(), 0);

        // Now half of them
        copy = newIndexBySearch();
        copy.search().setTypes("source").setQuery(termQuery("foo", "a"));
        copy.index().setIndex(destinationIndex).setType("dest_half");
        assertThat(copy.get(), responseMatcher().created(2));
        refresh();
        assertHitCount(client().prepareSearch(destinationIndex).setTypes("dest_half").setSize(0).get(), 2);

        // Limit with size
        copy = newIndexBySearch();
        copy.search().setTypes("source");
        copy.index().setIndex(destinationIndex).setType("dest_size_one");
        copy.size(1);
        assertThat(copy.get(), responseMatcher().created(1));
        refresh();
        assertHitCount(client().prepareSearch(destinationIndex).setTypes("dest_size_one").setSize(0).get(), 1);
    }

    public void testCopyMany() throws Exception {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int max = between(150, 500);
        for (int i = 0; i < max; i++) {
            docs.add(client().prepareIndex("test", "source", Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertHitCount(client().prepareSearch("test").setTypes("source").setSize(0).get(), max);

        // Copy all the docs
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.index().setIndex("test").setType("dest_all");
        assertThat(copy.get(), responseMatcher().created(max));
        refresh();
        assertHitCount(client().prepareSearch("test").setTypes("dest_all").setSize(0).get(), max);

        // Copy some of the docs
        int half = max / 2;
        copy = newIndexBySearch();
        copy.index().setIndex("test").setType("dest_half");
        // Use a small batch size so we have to use more than one batch
        copy.search().setSize(5);
        copy.size(half); // The real "size" of the request.
        assertThat(copy.get(), responseMatcher().created(half));
        refresh();
        assertHitCount(client().prepareSearch("test").setTypes("dest_half").setSize(0).get(), half);
    }
}
