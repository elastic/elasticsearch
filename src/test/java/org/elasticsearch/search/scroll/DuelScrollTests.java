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

package org.elasticsearch.search.scroll;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DuelScrollTests extends ElasticsearchIntegrationTest {

    @Test
    public void testDuel_queryThenFetch() throws Exception {
        TestContext context = create(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH);

        SearchResponse control = client().prepareSearch("index")
                .setSearchType(context.searchType)
                .addSort(context.sort)
                .setSize(context.numDocs).get();
        assertNoFailures(control);
        SearchHits sh = control.getHits();
        assertThat(sh.totalHits(), equalTo((long) context.numDocs));
        assertThat(sh.getHits().length, equalTo(context.numDocs));

        SearchResponse searchScrollResponse = client().prepareSearch("index")
                .setSearchType(context.searchType)
                .addSort(context.sort)
                .setSize(context.scrollRequestSize)
                .setScroll("10m").get();

        assertNoFailures(searchScrollResponse);
        assertThat(searchScrollResponse.getHits().getTotalHits(), equalTo((long) context.numDocs));
        assertThat(searchScrollResponse.getHits().hits().length, equalTo(context.scrollRequestSize));

        int counter = 0;
        for (SearchHit hit : searchScrollResponse.getHits()) {
            assertThat(hit.sortValues()[0], equalTo(sh.getAt(counter++).sortValues()[0]));
        }

        int iter = 1;
        String scrollId = searchScrollResponse.getScrollId();
        while (true) {
            searchScrollResponse = client().prepareSearchScroll(scrollId).setScroll("10m").get();
            assertNoFailures(searchScrollResponse);
            assertThat(searchScrollResponse.getHits().getTotalHits(), equalTo((long) context.numDocs));
            if (searchScrollResponse.getHits().hits().length == 0) {
                break;
            }

            int expectedLength;
            int scrollSlice = ++iter * context.scrollRequestSize;
            if (scrollSlice <= context.numDocs) {
                expectedLength = context.scrollRequestSize;
            } else {
                expectedLength = context.scrollRequestSize - (scrollSlice - context.numDocs);
            }
            assertThat(searchScrollResponse.getHits().hits().length, equalTo(expectedLength));
            for (SearchHit hit : searchScrollResponse.getHits()) {
                assertThat(hit.sortValues()[0], equalTo(sh.getAt(counter++).sortValues()[0]));
            }
            scrollId = searchScrollResponse.getScrollId();
        }

        assertThat(counter, equalTo(context.numDocs));
        clearScroll(scrollId);
    }

    @Test
    public void testDuel_queryAndFetch() throws Exception {
        // *_QUERY_AND_FETCH search types are tricky: the ordering can be incorrect, since it returns num_shards * (from + size)
        // a subsequent scroll call can return hits that should have been in the hits of the first scroll call.

        TestContext context = create(SearchType.DFS_QUERY_AND_FETCH, SearchType.QUERY_AND_FETCH);
        SearchResponse searchScrollResponse = client().prepareSearch("index")
                .setSearchType(context.searchType)
                .addSort(context.sort)
                .setSize(context.scrollRequestSize)
                .setScroll("10m").get();

        assertNoFailures(searchScrollResponse);
        assertThat(searchScrollResponse.getHits().getTotalHits(), equalTo((long) context.numDocs));

        int counter = searchScrollResponse.getHits().hits().length;
        String scrollId = searchScrollResponse.getScrollId();
        while (true) {
            searchScrollResponse = client().prepareSearchScroll(scrollId).setScroll("10m").get();
            assertNoFailures(searchScrollResponse);
            assertThat(searchScrollResponse.getHits().getTotalHits(), equalTo((long) context.numDocs));
            if (searchScrollResponse.getHits().hits().length == 0) {
                break;
            }

            counter += searchScrollResponse.getHits().hits().length;
            scrollId = searchScrollResponse.getScrollId();
        }

        assertThat(counter, equalTo(context.numDocs));
        clearScroll(scrollId);
    }


    private TestContext create(SearchType... searchTypes) throws Exception {
        assertAcked(prepareCreate("index").addMapping("type", jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("field1")
                    .field("type", "long")
                .endObject()
                .startObject("field2")
                    .field("type", "string")
                .endObject()
                .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("field3")
                            .field("type", "long")
                        .endObject()
                        .startObject("field4")
                            .field("type", "string")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()));

        int numDocs = 2 + randomInt(512);
        int scrollRequestSize = randomIntBetween(1, rarely() ? numDocs : numDocs / 2);
        boolean unevenRouting = randomBoolean();

        int numMissingDocs = scaledRandomIntBetween(0, numDocs / 100);
        IntOpenHashSet missingDocs = new IntOpenHashSet(numMissingDocs);
        for (int i = 0; i < numMissingDocs; i++) {
            while (!missingDocs.add(randomInt(numDocs))) {}
        }

        for (int i = 1; i <= numDocs; i++) {
            IndexRequestBuilder indexRequestBuilder = client()
                    .prepareIndex("index", "type", String.valueOf(i));
            if (missingDocs.contains(i)) {
                indexRequestBuilder.setSource("x", "y");
            } else {
                indexRequestBuilder.setSource(jsonBuilder().startObject()
                        .field("field1", i)
                        .field("field2", String.valueOf(i))
                        .startObject("nested")
                            .field("field3", i)
                            .field("field4", String.valueOf(i))
                        .endObject()
                        .endObject());
            }

            if (unevenRouting && randomInt(3) <= 2) {
                indexRequestBuilder.setRouting("a");
            }
            indexRandom(false, indexRequestBuilder);
        }
        refresh();

        final SortBuilder sort;
        if (randomBoolean()) {
            if (randomBoolean()) {
                sort = SortBuilders.fieldSort("field1").missing(1);
            } else {
                sort = SortBuilders.fieldSort("field2")
                        .missing("1");
            }
        } else {
            if (randomBoolean()) {
                sort = SortBuilders.fieldSort("nested.field3").missing(1);
            } else {
                sort = SortBuilders.fieldSort("nested.field4").missing("1");
            }
        }
        sort.order(randomBoolean() ? SortOrder.ASC : SortOrder.DESC);

        SearchType searchType = RandomPicks.randomFrom(getRandom(), Arrays.asList(searchTypes));

        logger.info("numDocs={}, scrollRequestSize={}, sort={}, searchType={}", numDocs, scrollRequestSize, sort, searchType);
        return new TestContext(numDocs, scrollRequestSize, sort, searchType);
    }


    class TestContext {

        final int numDocs;
        final int scrollRequestSize;
        final SortBuilder sort;
        final SearchType searchType;

        TestContext(int numDocs, int scrollRequestSize, SortBuilder sort, SearchType searchType) {
            this.numDocs = numDocs;
            this.scrollRequestSize = scrollRequestSize;
            this.sort = sort;
            this.searchType = searchType;
        }
    }

}
