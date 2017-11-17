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
package org.elasticsearch.validate;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
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
        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("foo").field("type", "text").endObject()
                        .startObject("bar").field("type", "integer").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        refresh();

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.wrapperQuery("foo".getBytes(StandardCharsets.UTF_8))).execute().actionGet().isValid(), equalTo(false));
        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("_id:1")).execute().actionGet().isValid(), equalTo(true));
        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("_i:d:1")).execute().actionGet().isValid(), equalTo(false));

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("foo:1")).execute().actionGet().isValid(), equalTo(true));
        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("bar:hey").lenient(false)).execute().actionGet().isValid(), equalTo(false));

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("nonexistent:hello")).execute().actionGet().isValid(), equalTo(true));

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryStringQuery("foo:1 AND")).execute().actionGet().isValid(), equalTo(false));
    }

    public void testExplainValidateQueryTwoNodes() throws IOException {
        createIndex("test");
        ensureGreen();
        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("foo").field("type", "text").endObject()
                        .startObject("bar").field("type", "integer").endObject()
                        .startObject("baz").field("type", "text").field("analyzer", "snowball").endObject()
                        .startObject("pin").startObject("properties").startObject("location").field("type", "geo_point").endObject().endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        refresh();

        for (Client client : internalCluster().getClients()) {
            ValidateQueryResponse response = client.admin().indices().prepareValidateQuery("test")
                    .setQuery(QueryBuilders.wrapperQuery("foo".getBytes(StandardCharsets.UTF_8)))
                    .setExplain(true)
                    .execute().actionGet();
            assertThat(response.isValid(), equalTo(false));
            assertThat(response.getQueryExplanation().size(), equalTo(1));
            assertThat(response.getQueryExplanation().get(0).getError(), containsString("Failed to derive xcontent"));
            assertThat(response.getQueryExplanation().get(0).getExplanation(), nullValue());

        }

        for (Client client : internalCluster().getClients()) {
            ValidateQueryResponse response = client.admin().indices().prepareValidateQuery("test")
                    .setQuery(QueryBuilders.queryStringQuery("foo"))
                    .setExplain(true)
                    .execute().actionGet();
            assertThat(response.isValid(), equalTo(true));
            assertThat(response.getQueryExplanation().size(), equalTo(1));
            assertThat(response.getQueryExplanation().get(0).getExplanation(), equalTo("(MatchNoDocsQuery(\"failed [bar] query, caused by number_format_exception:[For input string: \"foo\"]\") | foo:foo | baz:foo)"));
            assertThat(response.getQueryExplanation().get(0).getError(), nullValue());
        }
    }

    // Issue #3629
    public void testExplainDateRangeInQueryString() {
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                .put(indexSettings())
                .put("index.number_of_shards", 1)));

        String aMonthAgo = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).minusMonths(1));
        String aMonthFromNow = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).plusMonths(1));

        client().prepareIndex("test", "type", "1").setSource("past", aMonthAgo, "future", aMonthFromNow).get();

        refresh();

        ValidateQueryResponse response = client().admin().indices().prepareValidateQuery()
                .setQuery(queryStringQuery("past:[now-2M/d TO now/d]")).setRewrite(true).get();

        assertNoFailures(response);
        assertThat(response.getQueryExplanation().size(), equalTo(1));
        assertThat(response.getQueryExplanation().get(0).getError(), nullValue());
        DateTime twoMonthsAgo = new DateTime(DateTimeZone.UTC).minusMonths(2).withTimeAtStartOfDay();
        DateTime now = new DateTime(DateTimeZone.UTC).plusDays(1).withTimeAtStartOfDay().minusMillis(1);
        assertThat(response.getQueryExplanation().get(0).getExplanation(),
                equalTo("past:[" + twoMonthsAgo.getMillis() + " TO " + now.getMillis() + "]"));
        assertThat(response.isValid(), equalTo(true));
    }

    public void testValidateEmptyCluster() {
        try {
            client().admin().indices().prepareValidateQuery().get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testExplainNoQuery() {
        createIndex("test");
        ensureGreen();

        ValidateQueryResponse validateQueryResponse = client().admin().indices().prepareValidateQuery().setExplain(true).get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getIndex(), equalTo("test"));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), equalTo("*:*"));
    }

    public void testExplainFilteredAlias() {
        assertAcked(prepareCreate("test")
                .addMapping("test", "field", "type=text")
                .addAlias(new Alias("alias").filter(QueryBuilders.termQuery("field", "value1"))));
        ensureGreen();

        ValidateQueryResponse validateQueryResponse = client().admin().indices().prepareValidateQuery("alias")
                .setQuery(QueryBuilders.matchAllQuery()).setExplain(true).get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getIndex(), equalTo("test"));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), containsString("field:value1"));
    }

    public void testExplainMatchPhrasePrefix() {
        assertAcked(prepareCreate("test").setSettings(
                Settings.builder().put(indexSettings())
                        .put("index.analysis.filter.syns.type", "synonym")
                        .putList("index.analysis.filter.syns.synonyms", "one,two")
                        .put("index.analysis.analyzer.syns.tokenizer", "standard")
                        .putList("index.analysis.analyzer.syns.filter", "syns")
                    ).addMapping("test", "field","type=text,analyzer=syns"));
        ensureGreen();

        ValidateQueryResponse validateQueryResponse = client().admin().indices().prepareValidateQuery("test")
                .setQuery(QueryBuilders.matchPhrasePrefixQuery("field", "foo")).setExplain(true).get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), containsString("field:\"foo*\""));

        validateQueryResponse = client().admin().indices().prepareValidateQuery("test")
                .setQuery(QueryBuilders.matchPhrasePrefixQuery("field", "foo bar")).setExplain(true).get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), containsString("field:\"foo bar*\""));

        // Stacked tokens
        validateQueryResponse = client().admin().indices().prepareValidateQuery("test")
                .setQuery(QueryBuilders.matchPhrasePrefixQuery("field", "one bar")).setExplain(true).get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), containsString("field:\"(one two) bar*\""));

        validateQueryResponse = client().admin().indices().prepareValidateQuery("test")
                .setQuery(QueryBuilders.matchPhrasePrefixQuery("field", "foo one")).setExplain(true).get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), containsString("field:\"foo (one* two*)\""));
    }

    public void testExplainWithRewriteValidateQuery() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field", "type=text,analyzer=whitespace")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1)).get();
        client().prepareIndex("test", "type1", "1").setSource("field", "quick lazy huge brown pidgin").get();
        client().prepareIndex("test", "type1", "2").setSource("field", "the quick brown fox").get();
        client().prepareIndex("test", "type1", "3").setSource("field", "the quick lazy huge brown fox jumps over the tree").get();
        client().prepareIndex("test", "type1", "4").setSource("field", "the lazy dog quacks like a duck").get();
        refresh();

        // prefix queries
        assertExplanation(QueryBuilders.matchPhrasePrefixQuery("field", "qu"),
                containsString("field:quick"), true);
        assertExplanation(QueryBuilders.matchPhrasePrefixQuery("field", "ju"),
                containsString("field:jumps"), true);

        // common terms queries
        assertExplanation(QueryBuilders.commonTermsQuery("field", "huge brown pidgin").cutoffFrequency(1),
                containsString("+field:pidgin (field:huge field:brown)"), true);
        assertExplanation(QueryBuilders.commonTermsQuery("field", "the brown").analyzer("stop"),
                containsString("field:brown"), true);

        // match queries with cutoff frequency
        assertExplanation(QueryBuilders.matchQuery("field", "huge brown pidgin").cutoffFrequency(1),
                containsString("+field:pidgin (field:huge field:brown)"), true);
        assertExplanation(QueryBuilders.matchQuery("field", "the brown").analyzer("stop"),
                containsString("field:brown"), true);

        // fuzzy queries
        assertExplanation(QueryBuilders.fuzzyQuery("field", "the").fuzziness(Fuzziness.fromEdits(2)),
                containsString("field:the (field:tree)^0.3333333"), true);
        assertExplanation(QueryBuilders.fuzzyQuery("field", "jump"),
                containsString("(field:jumps)^0.75"), true);

        // more like this queries
        assertExplanation(QueryBuilders.moreLikeThisQuery(new String[] { "field" }, null, MoreLikeThisQueryBuilder.ids("1"))
                        .include(true).minTermFreq(1).minDocFreq(1).maxQueryTerms(2),
                containsString("field:huge field:pidgin"), true);
        assertExplanation(QueryBuilders.moreLikeThisQuery(new String[] { "field" }, new String[] {"the huge pidgin"}, null)
                        .minTermFreq(1).minDocFreq(1).maxQueryTerms(2),
                containsString("field:huge field:pidgin"), true);
    }

    public void testExplainWithRewriteValidateQueryAllShards() throws Exception {
        client().admin().indices().prepareCreate("test")
            .addMapping("type1", "field", "type=text,analyzer=whitespace")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2)).get();
        // We are relying on specific routing behaviors for the result to be right, so
        // we cannot randomize the number of shards or change ids here.
        client().prepareIndex("test", "type1", "1")
            .setSource("field", "quick lazy huge brown pidgin").get();
        client().prepareIndex("test", "type1", "2")
            .setSource("field", "the quick brown fox").get();
        client().prepareIndex("test", "type1", "3")
            .setSource("field", "the quick lazy huge brown fox jumps over the tree").get();
        client().prepareIndex("test", "type1", "4")
            .setSource("field", "the lazy dog quacks like a duck").get();
        refresh();

        // prefix queries
        assertExplanations(QueryBuilders.matchPhrasePrefixQuery("field", "qu"),
            Arrays.asList(
                equalTo("field:quick"),
                allOf(containsString("field:quick"), containsString("field:quacks"))
            ), true, true);
        assertExplanations(QueryBuilders.matchPhrasePrefixQuery("field", "ju"),
            Arrays.asList(
                equalTo("field:jumps"),
                equalTo("field:\"ju*\"")
            ), true, true);
    }

    public void testIrrelevantPropertiesBeforeQuery() throws IOException {
        createIndex("test");
        ensureGreen();
        refresh();

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.wrapperQuery(new BytesArray("{\"foo\": \"bar\", \"query\": {\"term\" : { \"user\" : \"kimchy\" }}}"))).get().isValid(), equalTo(false));
    }

    public void testIrrelevantPropertiesAfterQuery() throws IOException {
        createIndex("test");
        ensureGreen();
        refresh();

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.wrapperQuery(new BytesArray("{\"query\": {\"term\" : { \"user\" : \"kimchy\" }}, \"foo\": \"bar\"}"))).get().isValid(), equalTo(false));
    }

    private static void assertExplanation(QueryBuilder queryBuilder, Matcher<String> matcher, boolean withRewrite) {
        ValidateQueryResponse response = client().admin().indices().prepareValidateQuery("test")
                .setTypes("type1")
                .setQuery(queryBuilder)
                .setExplain(true)
                .setRewrite(withRewrite)
                .execute().actionGet();
        assertThat(response.getQueryExplanation().size(), equalTo(1));
        assertThat(response.getQueryExplanation().get(0).getError(), nullValue());
        assertThat(response.getQueryExplanation().get(0).getExplanation(), matcher);
        assertThat(response.isValid(), equalTo(true));
    }

    private static void assertExplanations(QueryBuilder queryBuilder,
                                           List<Matcher<String>> matchers, boolean withRewrite,
                                           boolean allShards) {
        ValidateQueryResponse response = client().admin().indices().prepareValidateQuery("test")
            .setTypes("type1")
            .setQuery(queryBuilder)
            .setExplain(true)
            .setRewrite(withRewrite)
            .setAllShards(allShards)
            .execute().actionGet();
        assertThat(response.getQueryExplanation().size(), equalTo(matchers.size()));
        for (int i = 0; i < matchers.size(); i++) {
            assertThat(response.getQueryExplanation().get(i).getError(), nullValue());
            assertThat(response.getQueryExplanation().get(i).getExplanation(), matchers.get(i));
            assertThat(response.isValid(), equalTo(true));
        }
    }
}
