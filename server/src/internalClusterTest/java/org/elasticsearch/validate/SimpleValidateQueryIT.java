/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.validate;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xcontent.XContentFactory;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.SUITE)
public class SimpleValidateQueryIT extends ESIntegTestCase {
    public void testSimpleValidateQuery() throws Exception {
        createIndex("test");
        ensureGreen();
        indicesAdmin().preparePutMapping("test")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "text")
                    .endObject()
                    .startObject("bar")
                    .field("type", "integer")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        refresh();

        assertThat(
            indicesAdmin().prepareValidateQuery("test")
                .setQuery(QueryBuilders.wrapperQuery("foo".getBytes(StandardCharsets.UTF_8)))
                .get()
                .isValid(),
            equalTo(false)
        );
        assertThat(
            indicesAdmin().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("_id:1")).get().isValid(),
            equalTo(true)
        );
        assertThat(
            indicesAdmin().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("_i:d:1")).get().isValid(),
            equalTo(false)
        );

        assertThat(
            indicesAdmin().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("foo:1")).get().isValid(),
            equalTo(true)
        );
        assertThat(
            indicesAdmin().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("bar:hey").lenient(false)).get().isValid(),
            equalTo(false)
        );

        assertThat(
            indicesAdmin().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("nonexistent:hello")).get().isValid(),
            equalTo(true)
        );

        assertThat(
            indicesAdmin().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("foo:1 AND")).get().isValid(),
            equalTo(false)
        );
    }

    public void testExplainValidateQueryTwoNodes() throws IOException {
        createIndex("test", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).build());
        ensureGreen();
        indicesAdmin().preparePutMapping("test")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "text")
                    .endObject()
                    .startObject("bar")
                    .field("type", "integer")
                    .endObject()
                    .startObject("baz")
                    .field("type", "text")
                    .field("analyzer", "standard")
                    .endObject()
                    .startObject("pin")
                    .startObject("properties")
                    .startObject("location")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setSource("foo", "text", "bar", i, "baz", "blort").setId(Integer.toString(i)).get();
        }
        refresh();

        for (Client client : internalCluster().getClients()) {
            ValidateQueryResponse response = client.admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.wrapperQuery("foo".getBytes(StandardCharsets.UTF_8)))
                .setExplain(true)
                .get();
            assertThat(response.isValid(), equalTo(false));
            assertThat(response.getQueryExplanation().size(), equalTo(1));
            assertThat(response.getQueryExplanation().get(0).getError(), containsString("Failed to derive xcontent"));
            assertThat(response.getQueryExplanation().get(0).getExplanation(), nullValue());

        }

        for (Client client : internalCluster().getClients()) {
            ValidateQueryResponse response = client.admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.queryStringQuery("foo"))
                .setExplain(true)
                .get();
            assertThat(response.isValid(), equalTo(true));
            assertThat(response.getQueryExplanation().size(), equalTo(1));
            assertThat(
                response.getQueryExplanation().get(0).getExplanation(),
                containsString("MatchNoDocsQuery(\"failed [bar] query, caused by number_format_exception:[For input string: \"foo\"]\")")
            );
            assertThat(response.getQueryExplanation().get(0).getExplanation(), containsString("foo:foo"));
            assertThat(response.getQueryExplanation().get(0).getExplanation(), containsString("baz:foo"));
            assertThat(response.getQueryExplanation().get(0).getError(), nullValue());
        }
    }

    // Issue #3629
    public void testExplainDateRangeInQueryString() {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.number_of_shards", 1)));

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String aMonthAgo = DateTimeFormatter.ISO_LOCAL_DATE.format(now.plus(1, ChronoUnit.MONTHS));
        String aMonthFromNow = DateTimeFormatter.ISO_LOCAL_DATE.format(now.minus(1, ChronoUnit.MONTHS));

        prepareIndex("test").setId("1").setSource("past", aMonthAgo, "future", aMonthFromNow).get();

        refresh();

        ValidateQueryResponse response = indicesAdmin().prepareValidateQuery()
            .setQuery(queryStringQuery("past:[now-2M/d TO now/d]"))
            .setRewrite(true)
            .get();

        assertNoFailures(response);
        assertThat(response.getQueryExplanation().size(), equalTo(1));
        assertThat(response.getQueryExplanation().get(0).getError(), nullValue());

        long twoMonthsAgo = now.minus(2, ChronoUnit.MONTHS).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000;
        long rangeEnd = (now.plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000) - 1;
        assertThat(
            response.getQueryExplanation().get(0).getExplanation(),
            containsString("past:[" + twoMonthsAgo + " TO " + rangeEnd + "]")
        );
        assertThat(response.isValid(), equalTo(true));
    }

    public void testValidateEmptyCluster() {
        ValidateQueryResponse response = indicesAdmin().prepareValidateQuery().get();
        assertThat(response.getTotalShards(), is(0));
    }

    public void testExplainNoQuery() {
        createIndex("test");
        ensureGreen();

        ValidateQueryResponse validateQueryResponse = indicesAdmin().prepareValidateQuery().setExplain(true).get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getIndex(), equalTo("test"));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), equalTo("*:*"));
    }

    public void testExplainFilteredAlias() {
        assertAcked(
            prepareCreate("test").setMapping("field", "type=text")
                .addAlias(new Alias("alias").filter(QueryBuilders.termQuery("field", "value1")))
        );
        ensureGreen();

        ValidateQueryResponse validateQueryResponse = indicesAdmin().prepareValidateQuery("alias")
            .setQuery(QueryBuilders.matchAllQuery())
            .setExplain(true)
            .get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getIndex(), equalTo("test"));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), containsString("field:value1"));
    }

    public void testExplainWithRewriteValidateQuery() {
        indicesAdmin().prepareCreate("test")
            .setMapping("field", "type=text,analyzer=whitespace")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1))
            .get();
        prepareIndex("test").setId("1").setSource("field", "quick lazy huge brown pidgin").get();
        prepareIndex("test").setId("2").setSource("field", "the quick brown fox").get();
        prepareIndex("test").setId("3").setSource("field", "the quick lazy huge brown fox jumps over the tree").get();
        prepareIndex("test").setId("4").setSource("field", "the lazy dog quacks like a duck").get();
        refresh();

        // prefix queries
        assertExplanation(QueryBuilders.matchPhrasePrefixQuery("field", "qu"), containsString("field:quick"), true);
        assertExplanation(QueryBuilders.matchPhrasePrefixQuery("field", "ju"), containsString("field:jumps"), true);

        // fuzzy queries
        assertExplanation(
            QueryBuilders.fuzzyQuery("field", "the").fuzziness(Fuzziness.fromEdits(2)),
            containsString("field:the (field:tree)^0.3333333"),
            true
        );
        assertExplanation(QueryBuilders.fuzzyQuery("field", "jump"), containsString("(field:jumps)^0.75"), true);

        // more like this queries
        Item[] items = new Item[] { new Item(null, "1") };
        assertExplanation(
            QueryBuilders.moreLikeThisQuery(new String[] { "field" }, null, items)
                .include(true)
                .minTermFreq(1)
                .minDocFreq(1)
                .maxQueryTerms(2),
            containsString("field:huge field:pidgin"),
            true
        );
        assertExplanation(
            QueryBuilders.moreLikeThisQuery(new String[] { "field" }, new String[] { "the huge pidgin" }, null)
                .minTermFreq(1)
                .minDocFreq(1)
                .maxQueryTerms(2),
            containsString("field:huge field:pidgin"),
            true
        );
    }

    public void testExplainWithRewriteValidateQueryAllShards() {
        indicesAdmin().prepareCreate("test")
            .setMapping("field", "type=text,analyzer=whitespace")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put("index.number_of_routing_shards", 2))
            .get();
        // We are relying on specific routing behaviors for the result to be right, so
        // we cannot randomize the number of shards or change ids here.
        prepareIndex("test").setId("1").setSource("field", "quick lazy huge brown pidgin").get();
        prepareIndex("test").setId("2").setSource("field", "the quick brown fox").get();
        prepareIndex("test").setId("3").setSource("field", "the quick lazy huge brown fox jumps over the tree").get();
        prepareIndex("test").setId("4").setSource("field", "the lazy dog quacks like a duck").get();
        refresh();

        // prefix queries
        assertExplanations(
            QueryBuilders.matchPhrasePrefixQuery("field", "qu"),
            Arrays.asList(equalTo("field:quick"), allOf(containsString("field:quick"), containsString("field:quacks"))),
            true,
            true
        );
        assertExplanations(
            QueryBuilders.matchPhrasePrefixQuery("field", "ju"),
            Arrays.asList(equalTo("field:jumps"), equalTo("field:\"ju*\"")),
            true,
            true
        );
    }

    public void testIrrelevantPropertiesBeforeQuery() {
        createIndex("test");
        ensureGreen();
        refresh();

        assertThat(indicesAdmin().prepareValidateQuery("test").setQuery(QueryBuilders.wrapperQuery(new BytesArray("""
            {"foo": "bar", "query": {"term" : { "user" : "kimchy" }}}
            """))).get().isValid(), equalTo(false));
    }

    public void testIrrelevantPropertiesAfterQuery() {
        createIndex("test");
        ensureGreen();
        refresh();

        assertThat(indicesAdmin().prepareValidateQuery("test").setQuery(QueryBuilders.wrapperQuery(new BytesArray("""
            {"query": {"term" : { "user" : "kimchy" }}, "foo": "bar"}
            """))).get().isValid(), equalTo(false));
    }

    private static void assertExplanation(QueryBuilder queryBuilder, Matcher<String> matcher, boolean withRewrite) {
        ValidateQueryResponse response = indicesAdmin().prepareValidateQuery("test")
            .setQuery(queryBuilder)
            .setExplain(true)
            .setRewrite(withRewrite)
            .get();
        assertThat(response.getQueryExplanation().size(), equalTo(1));
        assertThat(response.getQueryExplanation().get(0).getError(), nullValue());
        assertThat(response.getQueryExplanation().get(0).getExplanation(), matcher);
        assertThat(response.isValid(), equalTo(true));
    }

    private static void assertExplanations(
        QueryBuilder queryBuilder,
        List<Matcher<String>> matchers,
        boolean withRewrite,
        boolean allShards
    ) {
        ValidateQueryResponse response = indicesAdmin().prepareValidateQuery("test")
            .setQuery(queryBuilder)
            .setExplain(true)
            .setRewrite(withRewrite)
            .setAllShards(allShards)
            .get();
        assertThat(response.getQueryExplanation().size(), equalTo(matchers.size()));
        for (int i = 0; i < matchers.size(); i++) {
            assertThat(response.getQueryExplanation().get(i).getError(), nullValue());
            assertThat(response.getQueryExplanation().get(i).getExplanation(), matchers.get(i));
            assertThat(response.isValid(), equalTo(true));
        }
    }

    public void testExplainTermsQueryWithLookup() {
        indicesAdmin().prepareCreate("twitter")
            .setMapping("user", "type=integer", "followers", "type=integer")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put("index.number_of_routing_shards", 2))
            .get();
        prepareIndex("twitter").setId("1").setSource("followers", new int[] { 1, 2, 3 }).get();
        refresh();

        TermsQueryBuilder termsLookupQuery = QueryBuilders.termsLookupQuery("user", new TermsLookup("twitter", "1", "followers"));
        ValidateQueryResponse response = indicesAdmin().prepareValidateQuery("twitter").setQuery(termsLookupQuery).setExplain(true).get();
        assertThat(response.isValid(), is(true));
    }

    public void testOneClosedIndex() {
        createIndex("test");

        boolean ignoreUnavailable = false;
        IndicesOptions options = IndicesOptions.fromOptions(ignoreUnavailable, true, true, false, true, true, false, false);
        client().admin().indices().close(new CloseIndexRequest("test")).actionGet();
        IndexClosedException ex = expectThrows(
            IndexClosedException.class,
            indicesAdmin().prepareValidateQuery("test").setIndicesOptions(options)
        );
        assertEquals("closed", ex.getMessage());
    }

    public void testOneClosedIndexIgnoreUnavailable() {
        createIndex("test");

        boolean ignoreUnavailable = true;
        IndicesOptions options = IndicesOptions.fromOptions(ignoreUnavailable, true, true, false, true, true, false, false);
        client().admin().indices().close(new CloseIndexRequest("test")).actionGet();
        ValidateQueryResponse response = indicesAdmin().prepareValidateQuery("test").setIndicesOptions(options).get();
        assertThat(response.getTotalShards(), is(0));
    }

    public void testTwoIndicesOneClosed() {
        createIndex("test1");
        createIndex("test2");

        boolean ignoreUnavailable = false;
        IndicesOptions options = IndicesOptions.fromOptions(ignoreUnavailable, true, true, false, true, true, false, false);
        client().admin().indices().close(new CloseIndexRequest("test1")).actionGet();
        IndexClosedException ex = expectThrows(
            IndexClosedException.class,
            indicesAdmin().prepareValidateQuery("test1", "test2").setIndicesOptions(options)
        );
        assertEquals("closed", ex.getMessage());
    }

    public void testTwoIndicesOneClosedIgnoreUnavailable() {
        createIndex("test1");
        createIndex("test2");

        boolean ignoreUnavailable = true;
        IndicesOptions options = IndicesOptions.fromOptions(ignoreUnavailable, true, true, false, true, true, false, false);
        client().admin().indices().close(new CloseIndexRequest("test1")).actionGet();
        ValidateQueryResponse response = indicesAdmin().prepareValidateQuery("test1", "test2").setIndicesOptions(options).get();
        assertThat(response.getTotalShards(), is(1));
    }
}
