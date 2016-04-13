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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DuelScrollIT extends ESIntegTestCase {
    public void testDuelQueryThenFetch() throws Exception {
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

    public void testDuelQueryAndFetch() throws Exception {
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
                    .field("type", "keyword")
                .endObject()
                .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("field3")
                            .field("type", "long")
                        .endObject()
                        .startObject("field4")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()));

        int numDocs = 2 + randomInt(512);
        int scrollRequestSize = randomIntBetween(1, rarely() ? numDocs : numDocs / 2);
        boolean unevenRouting = randomBoolean();

        int numMissingDocs = scaledRandomIntBetween(0, numDocs / 100);
        IntHashSet missingDocs = new IntHashSet(numMissingDocs);
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

        SearchType searchType = RandomPicks.randomFrom(random(), Arrays.asList(searchTypes));

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

    private int createIndex(boolean singleShard) throws Exception {
        Settings.Builder settings = Settings.builder();
        if (singleShard) {
            settings.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1);
        }
        // no replicas, as they might be ordered differently
        settings.put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0);

        assertAcked(prepareCreate("test").setSettings(settings.build()).get());
        final int numDocs = randomIntBetween(10, 200);

        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; ++i) {
            builders[i] = client().prepareIndex("test", "type", Integer.toString(i)).setSource("foo", random().nextBoolean());
        }
        indexRandom(true, builders);
        return numDocs;
    }

    private void testDuelIndexOrder(SearchType searchType, boolean trackScores, int numDocs) throws Exception {
        final int size = scaledRandomIntBetween(5, numDocs + 5);
        final SearchResponse control = client().prepareSearch("test")
                .setSearchType(searchType)
                .setSize(numDocs)
                .setQuery(QueryBuilders.matchQuery("foo", "true"))
                .addSort(SortBuilders.fieldSort("_doc"))
                .setTrackScores(trackScores)
                .get();
        assertNoFailures(control);

        SearchResponse scroll = client().prepareSearch("test")
                .setSearchType(searchType)
                .setSize(size)
                .setQuery(QueryBuilders.matchQuery("foo", "true"))
                .addSort(SortBuilders.fieldSort("_doc"))
                .setTrackScores(trackScores)
                .setScroll("10m").get();

        int scrollDocs = 0;
        try {
            while (true) {
                assertNoFailures(scroll);
                assertEquals(control.getHits().getTotalHits(), scroll.getHits().getTotalHits());
                assertEquals(control.getHits().getMaxScore(), scroll.getHits().getMaxScore(), 0.01f);
                if (scroll.getHits().hits().length == 0) {
                    break;
                }
                for (int i = 0; i < scroll.getHits().hits().length; ++i) {
                    SearchHit controlHit = control.getHits().getAt(scrollDocs + i);
                    SearchHit scrollHit = scroll.getHits().getAt(i);
                    assertEquals(controlHit.getId(), scrollHit.getId());
                }
                scrollDocs += scroll.getHits().hits().length;
                scroll = client().prepareSearchScroll(scroll.getScrollId()).setScroll("10m").get();
            }
            assertEquals(control.getHits().getTotalHits(), scrollDocs);
        } catch (AssertionError e) {
            logger.info("Control:\n{}", control);
            logger.info("Scroll size={}, from={}:\n{}", size, scrollDocs, scroll);
            throw e;
        } finally {
            clearScroll(scroll.getScrollId());
        }
    }

    public void testDuelIndexOrderQueryAndFetch() throws Exception {
        final SearchType searchType = RandomPicks.randomFrom(random(), Arrays.asList(SearchType.QUERY_AND_FETCH, SearchType.DFS_QUERY_AND_FETCH));
        // QUERY_AND_FETCH only works with a single shard
        final int numDocs = createIndex(true);
        testDuelIndexOrder(searchType, false, numDocs);
        testDuelIndexOrder(searchType, true, numDocs);
    }

    public void testDuelIndexOrderQueryThenFetch() throws Exception {
        final SearchType searchType = RandomPicks.randomFrom(random(), Arrays.asList(SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH));
        final int numDocs = createIndex(false);
        testDuelIndexOrder(searchType, false, numDocs);
        testDuelIndexOrder(searchType, true, numDocs);
    }
}
