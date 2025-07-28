/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.simple;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

public class SimpleSearchIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), VerifyReaderContextPlugin.class);
    }

    public void testSearchNullIndex() {
        expectThrows(NullPointerException.class, () -> prepareSearch((String) null));
        expectThrows(NullPointerException.class, () -> prepareSearch((String[]) null));
    }

    public void testSearchRandomPreference() throws InterruptedException, ExecutionException {
        createIndex("test");
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field", "value"),
            prepareIndex("test").setId("2").setSource("field", "value"),
            prepareIndex("test").setId("3").setSource("field", "value"),
            prepareIndex("test").setId("4").setSource("field", "value"),
            prepareIndex("test").setId("5").setSource("field", "value"),
            prepareIndex("test").setId("6").setSource("field", "value")
        );

        int iters = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < iters; i++) {
            String randomPreference = randomUnicodeOfLengthBetween(0, 4);
            // randomPreference should not start with '_' (reserved for known preference types (e.g. _shards)
            while (randomPreference.startsWith("_")) {
                randomPreference = randomUnicodeOfLengthBetween(0, 4);
            }
            // id is not indexed, but lets see that we automatically convert to
            assertHitCount(prepareSearch().setQuery(QueryBuilders.matchAllQuery()).setPreference(randomPreference), 6L);
        }
    }

    public void testSimpleIp() throws Exception {
        createIndex("test");

        indicesAdmin().preparePutMapping("test")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("from")
                    .field("type", "ip")
                    .endObject()
                    .startObject("to")
                    .field("type", "ip")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("1").setSource("from", "192.168.0.5", "to", "192.168.0.10").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(
            prepareSearch().setQuery(boolQuery().must(rangeQuery("from").lte("192.168.0.7")).must(rangeQuery("to").gte("192.168.0.7"))),
            1L
        );
    }

    public void testIpCidr() throws Exception {
        createIndex("test");

        indicesAdmin().preparePutMapping("test")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("ip")
                    .field("type", "ip")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();

        prepareIndex("test").setId("1").setSource("ip", "192.168.0.1").get();
        prepareIndex("test").setId("2").setSource("ip", "192.168.0.2").get();
        prepareIndex("test").setId("3").setSource("ip", "192.168.0.3").get();
        prepareIndex("test").setId("4").setSource("ip", "192.168.1.4").get();
        prepareIndex("test").setId("5").setSource("ip", "2001:db8::ff00:42:8329").get();
        refresh();

        assertHitCount(prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.1.5/32"))), 0L);
        assertHitCount(
            1L,
            prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.1"))),
            prepareSearch().setQuery(queryStringQuery("ip: 192.168.0.1")),
            prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.1/32"))),
            prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "2001:db8::ff00:42:8329/128"))),
            prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "2001:db8::/64")))
        );
        assertHitCount(prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.0/24"))), 3L);
        assertHitCount(
            4L,
            prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.0.0.0/8"))),
            prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "0.0.0.0/0")))
        );
        assertHitCount(prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "::/0"))), 5L);

        assertFailures(
            prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "0/0/0/0/0"))),
            RestStatus.BAD_REQUEST,
            containsString("Expected [ip/prefix] but was [0/0/0/0/0]")
        );
    }

    public void testSimpleId() {
        createIndex("test");

        prepareIndex("test").setId("XXX1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        // id is not indexed, but lets see that we automatically convert to
        assertHitCount(
            1L,
            prepareSearch().setQuery(QueryBuilders.termQuery("_id", "XXX1")),
            prepareSearch().setQuery(QueryBuilders.queryStringQuery("_id:XXX1"))
        );
    }

    public void testSimpleDateRange() throws Exception {
        createIndex("test");
        prepareIndex("test").setId("1").setSource("field", "2010-01-05T02:00").get();
        prepareIndex("test").setId("2").setSource("field", "2010-01-06T02:00").get();
        prepareIndex("test").setId("3").setSource("field", "1967-01-01T00:00").get();
        ensureGreen();
        refresh();
        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-03||+2d").lte("2010-01-04||+2d/d")),
            2L
        );

        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05T02:00").lte("2010-01-06T02:00")),
            2L
        );

        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05T02:00").lt("2010-01-06T02:00")),
            1L
        );

        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gt("2010-01-05T02:00").lt("2010-01-06T02:00")),
            0L
        );

        assertHitCount(prepareSearch("test").setQuery(QueryBuilders.queryStringQuery("field:[2010-01-03||+2d TO 2010-01-04||+2d/d]")), 2L);

        // a string value of "1000" should be parsed as the year 1000 and return all three docs
        assertHitCountAndNoFailures(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gt("1000")), 3L);

        // a numeric value of 1000 should be parsed as 1000 millis since epoch and return only docs after 1970
        assertNoFailuresAndResponse(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gt(1000)), response -> {
            assertHitCount(response, 2L);
            String[] expectedIds = new String[] { "1", "2" };
            assertThat(response.getHits().getHits()[0].getId(), is(oneOf(expectedIds)));
            assertThat(response.getHits().getHits()[1].getId(), is(oneOf(expectedIds)));
        });
    }

    public void testRangeQueryKeyword() throws Exception {
        createIndex("test");

        indicesAdmin().preparePutMapping("test").setSource("field", "type=keyword").get();

        prepareIndex("test").setId("0").setSource("field", "").get();
        prepareIndex("test").setId("1").setSource("field", "A").get();
        prepareIndex("test").setId("2").setSource("field", "B").get();
        prepareIndex("test").setId("3").setSource("field", "C").get();
        ensureGreen();
        refresh();

        assertHitCountAndNoFailures(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("A").lte("B")), 2L);
        assertHitCountAndNoFailures(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gt("A").lte("B")), 1L);
        assertHitCountAndNoFailures(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("A").lt("B")), 1L);
        assertHitCountAndNoFailures(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte(null).lt("C")), 3L);
        assertHitCountAndNoFailures(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("B").lt(null)), 2L);
        assertHitCountAndNoFailures(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gt(null).lt(null)), 4L);
        assertHitCountAndNoFailures(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("").lt(null)), 4L);
        assertHitCountAndNoFailures(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gt("").lt(null)), 3L);
    }

    public void testSimpleTerminateAfterCount() throws Exception {
        prepareCreate("test").setSettings(indexSettings(1, 0)).get();
        ensureGreen();
        int max = randomIntBetween(3, 29);
        List<IndexRequestBuilder> docbuilders = new ArrayList<>(max);

        for (int i = 1; i <= max; i++) {
            String id = String.valueOf(i);
            docbuilders.add(prepareIndex("test").setId(id).setSource("field", i));
        }

        indexRandom(true, docbuilders);
        ensureGreen();
        refresh();

        for (int i = 1; i < max; i++) {
            final int finalI = i;
            assertResponse(
                prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max)).setTerminateAfter(i),
                response -> {
                    assertHitCount(response, finalI);
                    assertTrue(response.isTerminatedEarly());
                }
            );
        }
        assertResponse(
            prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max)).setTerminateAfter(2 * max),
            response -> {
                assertHitCount(response, max);
                assertFalse(response.isTerminatedEarly());
            }
        );
    }

    public void testSimpleIndexSortEarlyTerminate() throws Exception {
        prepareCreate("test").setSettings(indexSettings(1, 0).put("index.sort.field", "rank")).setMapping("rank", "type=integer").get();
        ensureGreen();
        int max = randomIntBetween(3, 29);
        List<IndexRequestBuilder> docbuilders = new ArrayList<>(max);

        for (int i = max - 1; i >= 0; i--) {
            String id = String.valueOf(i);
            docbuilders.add(prepareIndex("test").setId(id).setSource("rank", i));
        }

        indexRandom(true, docbuilders);
        ensureGreen();
        refresh();

        for (int i = 1; i < max; i++) {
            final int finalI = i;
            assertResponse(
                prepareSearch("test").addDocValueField("rank").setTrackTotalHits(false).addSort("rank", SortOrder.ASC).setSize(i),
                response -> {
                    assertNull(response.getHits().getTotalHits());
                    for (int j = 0; j < finalI; j++) {
                        assertThat(response.getHits().getAt(j).field("rank").getValue(), equalTo((long) j));
                    }
                }
            );
        }
    }

    public void testInsaneFromAndSize() throws Exception {
        createIndex("idx");
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertWindowFails(prepareSearch("idx").setFrom(Integer.MAX_VALUE));
        assertWindowFails(prepareSearch("idx").setSize(Integer.MAX_VALUE));
    }

    public void testTooLargeFromAndSize() throws Exception {
        createIndex("idx");
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertWindowFails(prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)));
        assertWindowFails(prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) + 1));
        assertWindowFails(
            prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
        );
    }

    public void testLargeFromAndSizeSucceeds() throws Exception {
        createIndex("idx");
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertHitCount(
            1,
            prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) - 10),
            prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)),
            prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) / 2)
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) / 2 - 1)
        );
    }

    public void testTooLargeFromAndSizeOkBySetting() throws Exception {
        prepareCreate("idx").setSettings(
            Settings.builder()
                .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 2)
        ).get();
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertHitCount(
            1,
            prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)),
            prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) + 1),
            prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
        );
    }

    public void testTooLargeFromAndSizeOkByDynamicSetting() throws Exception {
        createIndex("idx");
        updateIndexSettings(
            Settings.builder()
                .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 2),
            "idx"
        );
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertHitCount(
            1,
            prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)),
            prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) + 1),
            prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
        );
    }

    public void testTooLargeFromAndSizeBackwardsCompatibilityRecommendation() throws Exception {
        prepareCreate("idx").setSettings(Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), Integer.MAX_VALUE)).get();
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertHitCount(
            1,
            prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10),
            prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10),
            prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10)
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10)
        );
    }

    public void testTooLargeRescoreWindow() throws Exception {
        createIndex("idx");
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertRescoreWindowFails(Integer.MAX_VALUE);
        assertRescoreWindowFails(IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY) + 1);
    }

    public void testTooLargeRescoreOkBySetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        prepareCreate("idx").setSettings(Settings.builder().put(IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey(), defaultMaxWindow * 2))
            .get();
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertHitCount(prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)), 1);
    }

    public void testTooLargeRescoreOkByResultWindowSetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        prepareCreate("idx").setSettings(
            Settings.builder()
                .put(
                    IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), // Note that this is the RESULT window.
                    defaultMaxWindow * 2
                )
        ).get();
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertHitCount(prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)), 1);
    }

    public void testTooLargeRescoreOkByDynamicSetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        createIndex("idx");
        updateIndexSettings(Settings.builder().put(IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey(), defaultMaxWindow * 2), "idx");
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertHitCount(prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)), 1);
    }

    public void testTooLargeRescoreOkByDynamicResultWindowSetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        createIndex("idx");
        updateIndexSettings(
            // Note that this is the RESULT window
            Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), defaultMaxWindow * 2),
            "idx"
        );
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        assertHitCount(prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)), 1);
    }

    public void testQueryNumericFieldWithRegex() throws Exception {
        assertAcked(prepareCreate("idx").setMapping("num", "type=integer"));
        ensureGreen("idx");

        try {
            prepareSearch("idx").setQuery(QueryBuilders.regexpQuery("num", "34")).get();
            fail("SearchPhaseExecutionException should have been thrown");
        } catch (SearchPhaseExecutionException ex) {
            assertThat(ex.getRootCause().getMessage(), containsString("Can only use regexp queries on keyword and text fields"));
        }
    }

    public void testTermQueryBigInt() throws Exception {
        prepareCreate("idx").setMapping("field", "type=keyword").get();
        ensureGreen("idx");

        prepareIndex("idx").setId("1")
            .setSource("{\"field\" : 80315953321748200608 }", XContentType.JSON)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        String queryJson = "{ \"field\" : { \"value\" : 80315953321748200608 } }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, queryJson)) {
            parser.nextToken();
            TermQueryBuilder query = TermQueryBuilder.fromXContent(parser);
            assertHitCount(prepareSearch("idx").setQuery(query), 1);
        }
    }

    public void testTooLongRegexInRegexpQuery() throws Exception {
        createIndex("idx");
        indexRandom(true, prepareIndex("idx").setSource("{}", XContentType.JSON));

        int defaultMaxRegexLength = IndexSettings.MAX_REGEX_LENGTH_SETTING.get(Settings.EMPTY);
        StringBuilder regexp = new StringBuilder(defaultMaxRegexLength);
        while (regexp.length() <= defaultMaxRegexLength) {
            regexp.append("]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[\\t])*))*(?:,@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\");
        }
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            prepareSearch("idx").setQuery(QueryBuilders.regexpQuery("num", regexp.toString()))
        );
        assertThat(
            e.getRootCause().getMessage(),
            containsString(
                "The length of regex ["
                    + regexp.length()
                    + "] used in the Regexp Query request has exceeded "
                    + "the allowed maximum of ["
                    + defaultMaxRegexLength
                    + "]. "
                    + "This maximum can be set by changing the ["
                    + IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey()
                    + "] index level setting."
            )
        );
    }

    public void testTooLongPrefixInPrefixQuery() throws Exception {
        createIndex("idx");

        // Ensure the field `num` exists in the mapping
        client().admin()
            .indices()
            .preparePutMapping("idx")
            .setSource("{\"properties\":{\"num\":{\"type\":\"keyword\"}}}", XContentType.JSON)
            .get();

        // Index a simple document to ensure the field `num` is in the index
        indexRandom(true, prepareIndex("idx").setSource("{\"num\":\"test\"}", XContentType.JSON));

        int defaultMaxRegexLength = IndexSettings.MAX_REGEX_LENGTH_SETTING.get(Settings.EMPTY);
        StringBuilder prefix = new StringBuilder(defaultMaxRegexLength);

        while (prefix.length() <= defaultMaxRegexLength) {
            prefix.append("a");
        }

        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch("idx").setQuery(QueryBuilders.prefixQuery("num", prefix.toString())).get()
        );
        assertThat(
            e.getRootCause().getMessage(),
            containsString(
                "The length of prefix ["
                    + prefix.length()
                    + "] used in the Prefix Query request has exceeded "
                    + "the allowed maximum of ["
                    + defaultMaxRegexLength
                    + "]. "
                    + "This maximum can be set by changing the ["
                    + IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey()
                    + "] index level setting."
            )
        );
    }

    public void testStrictlyCountRequest() throws Exception {
        createIndex("test_count_1");
        indexRandom(
            true,
            prepareIndex("test_count_1").setId("1").setSource("field", "value"),
            prepareIndex("test_count_1").setId("2").setSource("field", "value"),
            prepareIndex("test_count_1").setId("3").setSource("field", "value"),
            prepareIndex("test_count_1").setId("4").setSource("field", "value"),
            prepareIndex("test_count_1").setId("5").setSource("field", "value"),
            prepareIndex("test_count_1").setId("6").setSource("field", "value")
        );

        createIndex("test_count_2");
        indexRandom(
            true,
            prepareIndex("test_count_2").setId("1").setSource("field", "value_2"),
            prepareIndex("test_count_2").setId("2").setSource("field", "value_2"),
            prepareIndex("test_count_2").setId("3").setSource("field", "value_2"),
            prepareIndex("test_count_2").setId("4").setSource("field", "value_2"),
            prepareIndex("test_count_2").setId("6").setSource("field", "value_2")
        );
        assertNoFailuresAndResponse(
            prepareSearch("test_count_1", "test_count_2").setTrackTotalHits(true).setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(11L));
                assertThat(response.getHits().getHits().length, equalTo(0));
            }
        );

    }

    private void assertWindowFails(SearchRequestBuilder search) {
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, search);
        assertThat(
            e.toString(),
            containsString(
                "Result window is too large, from + size must be less than or equal to: ["
                    + IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)
            )
        );
        assertThat(e.toString(), containsString("See the scroll api for a more efficient way to request large data sets"));
    }

    private void assertRescoreWindowFails(int windowSize) {
        SearchRequestBuilder search = prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(windowSize));
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, search);
        assertThat(
            e.toString(),
            containsString(
                "Rescore window ["
                    + windowSize
                    + "] is too large. It must "
                    + "be less than ["
                    + IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY)
            )
        );
        assertThat(
            e.toString(),
            containsString(
                "This limit can be set by changing the [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey() + "] index level setting."
            )
        );
    }

    public static class VerifyReaderContextPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onNewReaderContext(ReaderContext readerContext) {
                    assertThat(readerContext, not(instanceOf(LegacyReaderContext.class)));
                }

                @Override
                public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
                    assertThat(searchContext.readerContext(), not(instanceOf(LegacyReaderContext.class)));
                }

                @Override
                public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
                    assertThat(searchContext.readerContext(), not(instanceOf(LegacyReaderContext.class)));
                }
            });
        }
    }
}
