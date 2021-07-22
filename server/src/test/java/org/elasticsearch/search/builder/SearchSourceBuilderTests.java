/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.builder;

import com.fasterxml.jackson.core.JsonParseException;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class SearchSourceBuilderTests extends AbstractSearchTestCase {

    public void testFromXContent() throws IOException {
        SearchSourceBuilder testSearchSourceBuilder = createSearchSourceBuilder();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        testSearchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser xParser = createParser(builder)) {
            assertParseSearchSource(testSearchSourceBuilder, xParser);
        }
    }

    public void testFromXContentInvalid() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{}}")) {
            JsonParseException exc = expectThrows(JsonParseException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertThat(exc.getMessage(), containsString("Unexpected close marker"));
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{}{}")) {
            ParsingException exc = expectThrows(ParsingException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertThat(exc.getDetailedMessage(), containsString("found after the main object"));
        }
    }

    private static void assertParseSearchSource(SearchSourceBuilder testBuilder, XContentParser parser) throws IOException {
        if (randomBoolean()) {
            parser.nextToken(); // sometimes we move it on the START_OBJECT to
                                // test the embedded case
        }
        SearchSourceBuilder newBuilder = SearchSourceBuilder.fromXContent(parser);
        assertNull(parser.nextToken());
        assertEquals(testBuilder, newBuilder);
        assertEquals(testBuilder.hashCode(), newBuilder.hashCode());
    }

    public void testSerialization() throws IOException {
        SearchSourceBuilder original = createSearchSourceBuilder();
        SearchSourceBuilder copy = copyBuilder(original);
        assertEquals(copy, original);
        assertEquals(copy.hashCode(), original.hashCode());
        assertNotSame(copy, original);
    }

    public void testSerializingWithRuntimeFieldsBeforeSupportedThrows() {
        SearchSourceBuilder original = new SearchSourceBuilder().runtimeMappings(randomRuntimeMappings());
        Version v = VersionUtils.randomVersionBetween(
            random(),
            Version.V_7_11_0.minimumCompatibilityVersion(),
            VersionUtils.getPreviousVersion(Version.V_7_11_0)
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> copyBuilder(original, v));
        assertThat(e.getMessage(), equalTo("Versions before 7.11.0 don't support [runtime_mappings] and search was sent to [" + v + "]"));
    }

    public void testShallowCopy() {
        for (int i = 0; i < 10; i++) {
            SearchSourceBuilder original = createSearchSourceBuilder();
            SearchSourceBuilder copy = original.shallowCopy();
            assertEquals(original, copy);
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        // TODO add test checking that changing any member of this class produces an object that is not equal to the original
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createSearchSourceBuilder(), this::copyBuilder);
    }

    private SearchSourceBuilder copyBuilder(SearchSourceBuilder original) throws IOException {
        return copyBuilder(original, Version.CURRENT);
    }

    private SearchSourceBuilder copyBuilder(SearchSourceBuilder original, Version version) throws IOException {
        return ESTestCase.copyWriteable(original, namedWriteableRegistry, SearchSourceBuilder::new, version);
    }

    public void testParseIncludeExclude() throws IOException {
        {
            String restContent = " { \"_source\": { \"includes\": \"include\", \"excludes\": \"*.field2\"}}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                assertArrayEquals(new String[]{"*.field2"}, searchSourceBuilder.fetchSource().excludes());
                assertArrayEquals(new String[]{"include"}, searchSourceBuilder.fetchSource().includes());
            }
        }
        {
            String restContent = " { \"_source\": false}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                assertArrayEquals(new String[]{}, searchSourceBuilder.fetchSource().excludes());
                assertArrayEquals(new String[]{}, searchSourceBuilder.fetchSource().includes());
                assertFalse(searchSourceBuilder.fetchSource().fetchSource());
            }
        }
    }

    public void testMultipleQueryObjectsAreRejected() throws Exception {
        String restContent =
                " { \"query\": {\n" +
                "    \"multi_match\": {\n" +
                "      \"query\": \"workd\",\n" +
                "      \"fields\": [\"title^5\", \"plain_body\"]\n" +
                "    },\n" +
                "    \"filters\": {\n" +
                "      \"terms\": {\n" +
                "        \"status\": [ 3 ]\n" +
                "      }\n" +
                "    }\n" +
                "  } }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            ParsingException e = expectThrows(ParsingException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertEquals("[multi_match] malformed query, expected [END_OBJECT] but found [FIELD_NAME]", e.getMessage());
        }
    }

    public void testParseAndRewrite() throws IOException {
        String restContent = "{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"must\": {\n" +
            "        \"match_none\": {}\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"rescore\": {\n" +
            "    \"window_size\": 50,\n" +
            "    \"query\": {\n" +
            "      \"rescore_query\": {\n" +
            "        \"bool\": {\n" +
            "          \"must\": {\n" +
            "            \"match_none\": {}\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      \"rescore_query_weight\": 10\n" +
            "    }\n" +
            "  },\n" +
            "  \"highlight\": {\n" +
            "    \"order\": \"score\",\n" +
            "    \"fields\": {\n" +
            "      \"content\": {\n" +
            "        \"fragment_size\": 150,\n" +
            "        \"number_of_fragments\": 3,\n" +
            "        \"highlight_query\": {\n" +
            "          \"bool\": {\n" +
            "            \"must\": {\n" +
            "              \"match_none\": {}\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
            assertThat(searchSourceBuilder.query(), instanceOf(BoolQueryBuilder.class));
            assertThat(searchSourceBuilder.rescores().get(0), instanceOf(QueryRescorerBuilder.class));
            assertThat(((QueryRescorerBuilder)searchSourceBuilder.rescores().get(0)).getRescoreQuery(),
                instanceOf(BoolQueryBuilder.class));
            assertThat(searchSourceBuilder.highlighter().fields().get(0).highlightQuery(), instanceOf(BoolQueryBuilder.class));
            searchSourceBuilder = rewrite(searchSourceBuilder);

            assertThat(searchSourceBuilder.query(), instanceOf(MatchNoneQueryBuilder.class));
            assertThat(searchSourceBuilder.rescores().get(0), instanceOf(QueryRescorerBuilder.class));
            assertThat(((QueryRescorerBuilder)searchSourceBuilder.rescores().get(0)).getRescoreQuery(),
                instanceOf(MatchNoneQueryBuilder.class));
            assertThat(searchSourceBuilder.highlighter().fields().get(0).highlightQuery(), instanceOf(MatchNoneQueryBuilder.class));
            assertEquals(searchSourceBuilder.highlighter().fields().get(0).fragmentSize().intValue(), 150);
            assertEquals(searchSourceBuilder.highlighter().fields().get(0).numOfFragments().intValue(), 3);


        }

    }

    public void testParseSort() throws IOException {
        {
            String restContent = " { \"sort\": \"foo\"}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(1, searchSourceBuilder.sorts().size());
                assertEquals(new FieldSortBuilder("foo"), searchSourceBuilder.sorts().get(0));
            }
        }

        {
            String restContent = "{\"sort\" : [\n" +
                    "        { \"post_date\" : {\"order\" : \"asc\"}},\n" +
                    "        \"user\",\n" +
                    "        { \"name\" : \"desc\" },\n" +
                    "        { \"age\" : \"desc\" },\n" +
                    "        \"_score\"\n" +
                    "    ]}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(5, searchSourceBuilder.sorts().size());
                assertEquals(new FieldSortBuilder("post_date"), searchSourceBuilder.sorts().get(0));
                assertEquals(new FieldSortBuilder("user"), searchSourceBuilder.sorts().get(1));
                assertEquals(new FieldSortBuilder("name").order(SortOrder.DESC), searchSourceBuilder.sorts().get(2));
                assertEquals(new FieldSortBuilder("age").order(SortOrder.DESC), searchSourceBuilder.sorts().get(3));
                assertEquals(new ScoreSortBuilder(), searchSourceBuilder.sorts().get(4));
            }
        }
    }

    public void testAggsParsing() throws IOException {
        {
            String restContent = "{\n" + "    " +
                    "\"aggs\": {" +
                    "        \"test_agg\": {\n" +
                    "            " + "\"terms\" : {\n" +
                    "                \"field\": \"foo\"\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(1, searchSourceBuilder.aggregations().count());
            }
        }
        {
            String restContent = "{\n" +
                    "    \"aggregations\": {" +
                    "        \"test_agg\": {\n" +
                    "            \"terms\" : {\n" +
                    "                \"field\": \"foo\"\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(1, searchSourceBuilder.aggregations().count());
            }
        }
    }

    /**
     * test that we can parse the `rescore` element either as single object or as array
     */
    public void testParseRescore() throws IOException {
        {
            String restContent = "{\n" +
                "    \"query\" : {\n" +
                "        \"match\": { \"content\": { \"query\": \"foo bar\" }}\n" +
                "     },\n" +
                "    \"rescore\": {" +
                "        \"window_size\": 50,\n" +
                "        \"query\": {\n" +
                "            \"rescore_query\" : {\n" +
                "                \"match\": { \"content\": { \"query\": \"baz\" } }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(1, searchSourceBuilder.rescores().size());
                assertEquals(new QueryRescorerBuilder(QueryBuilders.matchQuery("content", "baz")).windowSize(50),
                        searchSourceBuilder.rescores().get(0));
            }
        }

        {
            String restContent = "{\n" +
                "    \"query\" : {\n" +
                "        \"match\": { \"content\": { \"query\": \"foo bar\" }}\n" +
                "     },\n" +
                "    \"rescore\": [ {" +
                "        \"window_size\": 50,\n" +
                "        \"query\": {\n" +
                "            \"rescore_query\" : {\n" +
                "                \"match\": { \"content\": { \"query\": \"baz\" } }\n" +
                "            }\n" +
                "        }\n" +
                "    } ]\n" +
                "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(1, searchSourceBuilder.rescores().size());
                assertEquals(new QueryRescorerBuilder(QueryBuilders.matchQuery("content", "baz")).windowSize(50),
                        searchSourceBuilder.rescores().get(0));
            }
        }
    }

    public void testTimeoutWithUnits() throws IOException {
        final String timeout = randomTimeValue();
        final String query = "{ \"query\": { \"match_all\": {}}, \"timeout\": \"" + timeout + "\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, query)) {
            final SearchSourceBuilder builder = SearchSourceBuilder.fromXContent(parser);
            assertThat(builder.timeout(), equalTo(TimeValue.parseTimeValue(timeout, null, "timeout")));
        }
    }

    public void testTimeoutWithoutUnits() throws IOException {
        final int timeout = randomIntBetween(1, 1024);
        final String query = "{ \"query\": { \"match_all\": {}}, \"timeout\": \"" + timeout + "\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, query)) {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SearchSourceBuilder.fromXContent(
                    parser));
            assertThat(e, hasToString(containsString("unit is missing or unrecognized")));
        }
    }

    public void testToXContent() throws IOException {
        //verify that only what is set gets printed out through toXContent
        XContentType xContentType = randomFrom(XContentType.values());
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
            searchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference source = BytesReference.bytes(builder);
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false, xContentType).v2();
            assertEquals(0, sourceAsMap.size());
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(RandomQueryBuilder.createQuery(random()));
            XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
            searchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference source = BytesReference.bytes(builder);
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false, xContentType).v2();
            assertEquals(1, sourceAsMap.size());
            assertEquals("query", sourceAsMap.keySet().iterator().next());
        }
    }

    public void testToXContentWithPointInTime() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TimeValue keepAlive = randomBoolean() ? TimeValue.timeValueHours(1) : null;
        searchSourceBuilder.pointInTimeBuilder(new PointInTimeBuilder("id").setKeepAlive(keepAlive));
        XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        searchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(builder);
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(bytes, false, xContentType).v2();
        assertEquals(1, sourceAsMap.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> pit = (Map<String, Object>) sourceAsMap.get("pit");
        assertEquals("id", pit.get("id"));
        if (keepAlive != null) {
            assertEquals("1h", pit.get("keep_alive"));
            assertEquals(2, pit.size());
        } else {
            assertNull(pit.get("keep_alive"));
            assertEquals(1, pit.size());
        }
    }

    public void testParseIndicesBoost() throws IOException {
        {
            String restContent = " { \"indices_boost\": {\"foo\": 1.0, \"bar\": 2.0}}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                assertEquals(2, searchSourceBuilder.indexBoosts().size());
                assertEquals(new SearchSourceBuilder.IndexBoost("foo", 1.0f), searchSourceBuilder.indexBoosts().get(0));
                assertEquals(new SearchSourceBuilder.IndexBoost("bar", 2.0f), searchSourceBuilder.indexBoosts().get(1));
                assertWarnings("Object format in indices_boost is deprecated, please use array format instead");
            }
        }

        {
            String restContent = "{" +
                "    \"indices_boost\" : [\n" +
                "        { \"foo\" : 1.0 },\n" +
                "        { \"bar\" : 2.0 },\n" +
                "        { \"baz\" : 3.0 }\n" +
                "    ]}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                assertEquals(3, searchSourceBuilder.indexBoosts().size());
                assertEquals(new SearchSourceBuilder.IndexBoost("foo", 1.0f), searchSourceBuilder.indexBoosts().get(0));
                assertEquals(new SearchSourceBuilder.IndexBoost("bar", 2.0f), searchSourceBuilder.indexBoosts().get(1));
                assertEquals(new SearchSourceBuilder.IndexBoost("baz", 3.0f), searchSourceBuilder.indexBoosts().get(2));
            }
        }

        {
            String restContent = "{" +
                "    \"indices_boost\" : [\n" +
                "        { \"foo\" : 1.0, \"bar\": 2.0}\n" + // invalid format
                "    ]}";

            assertIndicesBoostParseErrorMessage(restContent, "Expected [END_OBJECT] in [indices_boost] but found [FIELD_NAME]");
        }

        {
            String restContent = "{" +
                "    \"indices_boost\" : [\n" +
                "        {}\n" + // invalid format
                "    ]}";

            assertIndicesBoostParseErrorMessage(restContent, "Expected [FIELD_NAME] in [indices_boost] but found [END_OBJECT]");
        }

        {
            String restContent = "{" +
                "    \"indices_boost\" : [\n" +
                "        { \"foo\" : \"bar\"}\n" + // invalid format
                "    ]}";

            assertIndicesBoostParseErrorMessage(restContent, "Expected [VALUE_NUMBER] in [indices_boost] but found [VALUE_STRING]");
        }

        {
            String restContent = "{" +
                "    \"indices_boost\" : [\n" +
                "        { \"foo\" : {\"bar\": 1}}\n" + // invalid format
                "    ]}";

            assertIndicesBoostParseErrorMessage(restContent, "Expected [VALUE_NUMBER] in [indices_boost] but found [START_OBJECT]");
        }
    }

    public void testNegativeFromErrors() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> new SearchSourceBuilder().from(-2));
        assertEquals("[from] parameter cannot be negative", expected.getMessage());
    }

    public void testNegativeSizeErrors() throws IOException {
        int randomSize = randomIntBetween(-100000, -2);
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
                () -> new SearchSourceBuilder().size(randomSize));
        assertEquals("[size] parameter cannot be negative, found [" + randomSize + "]", expected.getMessage());
        expected = expectThrows(IllegalArgumentException.class, () -> new SearchSourceBuilder().size(-1));
        assertEquals("[size] parameter cannot be negative, found [-1]", expected.getMessage());

        // we don't want to error on -1 for bwc reasons, this is treated as if the value is unset later
        String restContent = "{\"size\" : -1}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
            assertEquals(-1, searchSourceBuilder.size());
        }
        assertWarnings("Using search size of -1 is deprecated and will be removed in future versions. Instead, don't use the `size` "
            + "parameter if you don't want to set it explicitly.");

        restContent = "{\"size\" : " + randomSize + "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertThat(ex.getMessage(), containsString(Integer.toString(randomSize)));
        }
    }

    public void testNegativeTerminateAfter() throws IOException {
        int randomNegativeValue = randomIntBetween(-100000, -1);
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
                () -> new SearchSourceBuilder().terminateAfter(randomNegativeValue));
        assertEquals("terminateAfter must be > 0", expected.getMessage());

        String restContent = "{\"terminate_after\" :" + randomNegativeValue + "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertThat(ex.getMessage(), containsString("terminateAfter must be > 0"));
        }
    }

    public void testNegativeTrackTotalHits() throws IOException {
        int randomNegativeValue = randomIntBetween(-100000, -2);
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
                () -> new SearchSourceBuilder().trackTotalHitsUpTo(randomNegativeValue));
        assertEquals("[track_total_hits] parameter must be positive or equals to -1, got " + randomNegativeValue, expected.getMessage());

        String restContent = "{\"track_total_hits\" :" + randomNegativeValue + "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertEquals("[track_total_hits] parameter must be positive or equals to -1, got " + randomNegativeValue, ex.getMessage());
        }
    }

    private void assertIndicesBoostParseErrorMessage(String restContent, String expectedErrorMessage) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            ParsingException e = expectThrows(ParsingException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertEquals(expectedErrorMessage, e.getMessage());
        }
    }

    private SearchSourceBuilder rewrite(SearchSourceBuilder searchSourceBuilder) throws IOException {
        return Rewriteable.rewrite(searchSourceBuilder, new QueryRewriteContext(xContentRegistry(), writableRegistry(),
                null, Long.valueOf(1)::longValue));
    }
}
