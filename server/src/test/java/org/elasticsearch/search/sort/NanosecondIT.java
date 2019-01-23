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

package org.elasticsearch.search.sort;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;

public class NanosecondIT extends ESIntegTestCase {

    private static final DateFormatter FORMATTER = DateFormatter.forPattern("strict_date_optional_time_nanos");

    private Settings settings;

    @Before
    public void setupSettings() {
        settings = Settings.builder()
        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, internalCluster().numDataNodes()-1))
            .build();
    }

    // this sorts against two indices with nanosecond mapping
    public void testSortingWithNanosecondsOnly() throws Exception {
        createIndices();
        XContentBuilder nanoBuilder = XContentFactory.jsonBuilder().startObject().startObject("doc").startObject("properties")
            .startObject("date").field("type", "nanosecond").endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("date_ns_2").setSettings(settings).addMapping("doc", nanoBuilder));
        ensureGreen();

        indexRandom(true,
            client().prepareIndex("date_ns", "doc", "1").setSource("date", "2018-10-29T12:12:12.123456789Z"),
            client().prepareIndex("date_ns_2", "doc", "2").setSource("date", "2018-10-29T12:12:12.987654321Z"));

        SortOrder sortOrder = randomFrom(SortOrder.values());
        SearchResponse response = client().prepareSearch("date_ns*").addSort("date", sortOrder).get();
        assertHitCount(response, 2);
        logger.info("response {}", Arrays.asList(response.getHits().getHits()));

        if (sortOrder == SortOrder.ASC) {
            assertSearchHit(response, 0, "1", DateUtils.toLong(toInstant("2018-10-29T12:12:12.123456789Z")));
            assertSearchHit(response, 1, "2", DateUtils.toLong(toInstant("2018-10-29T12:12:12.987654321Z")));
        }

        if (sortOrder == SortOrder.DESC) {
            assertSearchHit(response, 0, "2", DateUtils.toLong(toInstant("2018-10-29T12:12:12.987654321Z")));
            assertSearchHit(response, 1, "1", DateUtils.toLong(toInstant("2018-10-29T12:12:12.123456789Z")));
        }
    }

    // this sorts agains two nanosecond indices with data, and one millisecond index without
    public void testSortingWithEmptyMillisecondIndex() throws Exception {
        createIndices();
        XContentBuilder nanoBuilder = XContentFactory.jsonBuilder().startObject().startObject("doc").startObject("properties")
            .startObject("date").field("type", "nanosecond").endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("date_ns_2").setSettings(settings).addMapping("doc", nanoBuilder));
        ensureGreen();

        indexRandom(true, false,
            client().prepareIndex("date_ns", "doc", "1").setSource("date", "2018-10-29T12:12:12.123456789Z"),
            client().prepareIndex("date_ns_2", "doc", "2").setSource("date", "2018-10-29T12:12:12.987654321Z"));

        SearchResponse response = client().prepareSearch("date*").addSort("date", SortOrder.DESC).get();
        assertHitCount(response, 2);
        logger.info("response {}", Arrays.asList(response.getHits().getHits()));

        assertSearchHit(response, 0, "2", DateUtils.toLong(toInstant("2018-10-29T12:12:12.987654321Z")));
        assertSearchHit(response, 1, "1", DateUtils.toLong(toInstant("2018-10-29T12:12:12.123456789Z")));
    }

    // this sorts against two indices, one with nanosecond mapping, one with date mapping
    public void testSortingWithMixedIndices() throws Exception {
        createIndices();

        indexRandom(true,
            client().prepareIndex("date_ns", "doc", "1").setSource("date", "2018-10-29T12:12:12.123456789Z"),
            client().prepareIndex("date_ms", "doc", "2").setSource("date", "2018-10-29T12:12:12.987Z"));

        SortOrder sortOrder = randomFrom(SortOrder.values());
        SearchResponse response = client().prepareSearch("date*")
            .addSort("date", sortOrder)
            .get();
        assertHitCount(response, 2);
        logger.info("response {}", Arrays.asList(response.getHits().getHits()));

        if (sortOrder == SortOrder.DESC) {
            assertSearchHit(response, 0, "2", toInstant("2018-10-29T12:12:12.987Z").toEpochMilli());
            assertSearchHit(response, 1, "1", toInstant("2018-10-29T12:12:12.123456789Z").toEpochMilli());
        }
        if (sortOrder == SortOrder.ASC) {
            assertSearchHit(response, 0, "1", toInstant("2018-10-29T12:12:12.123456789Z").toEpochMilli());
            assertSearchHit(response, 1, "2", toInstant("2018-10-29T12:12:12.987Z").toEpochMilli());
        }
    }

    public void testSortingWithMixedIndicesAndSeveralSortValues() throws Exception {
        createIndices();

        indexRandom(true,
            client().prepareIndex("date_ns", "doc", "1").setSource("date", "2018-10-29T12:12:12.123456789Z", "sort1", 1),
            client().prepareIndex("date_ms", "doc", "2").setSource("date", "2018-10-29T12:12:12.987Z", "sort1", 1),
            client().prepareIndex("date_ns", "doc", "3").setSource("date", "2018-10-29T12:12:12.123456789Z", "sort1", 2),
            client().prepareIndex("date_ms", "doc", "4").setSource("date", "2018-10-29T12:12:12.987Z", "sort1", 2),
            client().prepareIndex("date_ns", "doc", "5").setSource("date", "2018-10-29T12:12:12.123456789Z", "sort1", 3),
            client().prepareIndex("date_ms", "doc", "6").setSource("date", "2018-10-29T12:12:12.987Z", "sort1", 3));

        SortOrder sortOrder = randomFrom(SortOrder.values());
        SearchResponse response = client().prepareSearch("date*")
            .addSort("sort1", SortOrder.DESC)
            .addSort("date", sortOrder)
            .get();
        assertHitCount(response, 6);

        long laterMillis = toInstant("2018-10-29T12:12:12.987Z").toEpochMilli();
        long earlierMillis = toInstant("2018-10-29T12:12:12.123456789Z").toEpochMilli();
        if (sortOrder == SortOrder.DESC) {
            assertSearchHit(response, 0, "6", 3L, laterMillis);
            assertSearchHit(response, 1, "5", 3L, earlierMillis);
            assertSearchHit(response, 2, "4", 2L, laterMillis);
            assertSearchHit(response, 3, "3", 2L, earlierMillis);
            assertSearchHit(response, 4, "2", 1L, laterMillis);
            assertSearchHit(response, 5, "1", 1L, earlierMillis);
        }
        if (sortOrder == SortOrder.ASC) {
            assertSearchHit(response, 0, "5", 3L, earlierMillis);
            assertSearchHit(response, 1, "6", 3L, laterMillis);
            assertSearchHit(response, 2, "3", 2L, earlierMillis);
            assertSearchHit(response, 3, "4", 2L, laterMillis);
            assertSearchHit(response, 4, "1", 1L, earlierMillis);
            assertSearchHit(response, 5, "2", 1L, laterMillis);
        }
    }

    public void testSortingWithRangeQuery() throws Exception {
        createIndices();

        indexRandom(true,
            client().prepareIndex("date_ns", "doc", "1").setSource("date", "2018-10-29T12:12:12.123456789Z"),
            client().prepareIndex("date_ms", "doc", "2").setSource("date", "2018-10-29T12:12:12.987Z"),
            client().prepareIndex("date_ns", "doc", "3").setSource("date", "2018-10-30T12:12:12.123456789Z"),
            client().prepareIndex("date_ms", "doc", "4").setSource("date", "2018-10-30T12:12:12.987Z"),
            client().prepareIndex("date_ns", "doc", "5").setSource("date", "2018-10-31T12:12:12.123456789Z"),
            client().prepareIndex("date_ms", "doc", "6").setSource("date", "2018-10-31T12:12:12.987Z"));

        SortOrder sortOrder = randomFrom(SortOrder.values());
        RangeQueryBuilder query = QueryBuilders.rangeQuery("date")
            .from("2018-10-30", true)
            .to("2018-10-31", false);
        SearchResponse response = client().prepareSearch("date*")
            .setQuery(query)
            .addSort("date", sortOrder)
            .get();
        assertHitCount(response, 2);

        if (sortOrder == SortOrder.ASC) {
            assertSearchHit(response, 0, "3", toInstant("2018-10-30T12:12:12.123456789Z").toEpochMilli());
            assertSearchHit(response, 1, "4", toInstant("2018-10-30T12:12:12.987Z").toEpochMilli());
        }
        if (sortOrder == SortOrder.DESC) {
            assertSearchHit(response, 0, "4", toInstant("2018-10-30T12:12:12.987Z").toEpochMilli());
            assertSearchHit(response, 1, "3", toInstant("2018-10-30T12:12:12.123456789Z").toEpochMilli());
        }
    }

    public void testPaginationWithMixedIndices() throws Exception {
        createIndices();

        indexRandom(true,
            client().prepareIndex("date_ns", "doc", "1").setSource("date", "2018-10-29T12:12:12.123456789Z"),
            client().prepareIndex("date_ms", "doc", "2").setSource("date", "2018-10-29T12:12:12.987Z"),
            client().prepareIndex("date_ns", "doc", "3").setSource("date", "2018-10-30T12:12:12.123456789Z"),
            client().prepareIndex("date_ms", "doc", "4").setSource("date", "2018-10-30T12:12:12.987Z"),
            client().prepareIndex("date_ns", "doc", "5").setSource("date", "2018-10-31T12:12:12.123456789Z"),
            client().prepareIndex("date_ms", "doc", "6").setSource("date", "2018-10-31T12:12:12.987Z"));

        SortOrder sortOrder = randomFrom(SortOrder.values());

        SearchResponse response = client().prepareSearch("date*").addSort("date", sortOrder).setSize(2).get();
        assertHitCount(response, 6);
        if (sortOrder == SortOrder.ASC) {
            assertSearchHit(response, 0, "1", toInstant("2018-10-29T12:12:12.123456789Z").toEpochMilli());
            assertSearchHit(response, 1, "2", toInstant("2018-10-29T12:12:12.987Z").toEpochMilli());
        }
        if (sortOrder == SortOrder.DESC) {
            assertSearchHit(response, 0, "6", toInstant("2018-10-31T12:12:12.987Z").toEpochMilli());
            assertSearchHit(response, 1, "5", toInstant("2018-10-31T12:12:12.123456789Z").toEpochMilli());
        }

        response = client().prepareSearch("date*").addSort("date", sortOrder).setSize(2).setFrom(2).get();
        assertHitCount(response, 6);
        if (sortOrder == SortOrder.ASC) {
            assertSearchHit(response, 0, "3", toInstant("2018-10-30T12:12:12.123456789Z").toEpochMilli());
            assertSearchHit(response, 1, "4", toInstant("2018-10-30T12:12:12.987Z").toEpochMilli());
        }
        if (sortOrder == SortOrder.DESC) {
            assertSearchHit(response, 0, "4", toInstant("2018-10-30T12:12:12.987Z").toEpochMilli());
            assertSearchHit(response, 1, "3", toInstant("2018-10-30T12:12:12.123456789Z").toEpochMilli());
        }

        response = client().prepareSearch("date*").addSort("date", sortOrder).setSize(3).setFrom(4).get();
        assertHitCount(response, 6);
        if (sortOrder == SortOrder.ASC) {
            assertSearchHit(response, 0, "5", toInstant("2018-10-31T12:12:12.123456789Z").toEpochMilli());
            assertSearchHit(response, 1, "6", toInstant("2018-10-31T12:12:12.987Z").toEpochMilli());
        }
        if (sortOrder == SortOrder.DESC) {
            assertSearchHit(response, 0, "2", toInstant("2018-10-29T12:12:12.987Z").toEpochMilli());
            assertSearchHit(response, 1, "1", toInstant("2018-10-29T12:12:12.123456789Z").toEpochMilli());
        }
    }

    private Instant toInstant(String input) {
        return DateFormatters.toZonedDateTime(FORMATTER.parse(input)).toInstant();
    }

    private void createIndices() throws IOException {
        XContentBuilder nanoBuilder = XContentFactory.jsonBuilder().startObject().startObject("doc").startObject("properties")
            .startObject("date").field("type", "nanosecond").endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("date_ns").setSettings(settings).addMapping("doc", nanoBuilder));
        XContentBuilder dateBuilder = XContentFactory.jsonBuilder().startObject().startObject("doc").startObject("properties")
            .startObject("date").field("type", "date").endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("date_ms").setSettings(settings).addMapping("doc", dateBuilder));
        ensureGreen();
    }

    private void assertSearchHit(SearchResponse response, int position, String expectedId, Object ... expectedSortValues) {
        SearchHit firstHit = response.getHits().getAt(position);
        assertThat(firstHit.getId(), is(expectedId));
        assertThat(firstHit.getSortValues(), is(arrayWithSize(expectedSortValues.length)));
        for (int i = 0; i < expectedSortValues.length; i++) {
            assertThat(firstHit.getSortValues()[i], is(expectedSortValues[i]));
        }
    }
}
