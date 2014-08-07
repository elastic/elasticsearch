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

import com.google.common.base.Charsets;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(randomDynamicTemplates = false)
public class SimpleValidateQueryTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleValidateQuery() throws Exception {
        createIndex("test");
        ensureGreen();
        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("foo").field("type", "string").endObject()
                        .startObject("bar").field("type", "integer").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        refresh();

        assertThat(client().admin().indices().prepareValidateQuery("test").setSource("foo".getBytes(Charsets.UTF_8)).execute().actionGet().isValid(), equalTo(false));
        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("_id:1")).execute().actionGet().isValid(), equalTo(true));
        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("_i:d:1")).execute().actionGet().isValid(), equalTo(false));

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("foo:1")).execute().actionGet().isValid(), equalTo(true));
        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("bar:hey")).execute().actionGet().isValid(), equalTo(false));

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("nonexistent:hello")).execute().actionGet().isValid(), equalTo(true));

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("foo:1 AND")).execute().actionGet().isValid(), equalTo(false));
    }

    private static String filter(String uncachedFilter) {
        String filter = uncachedFilter;
        if (cluster().hasFilterCache()) {
            filter = "cache(" + filter + ")";
        }
        return filter;
    }

    @Test
    public void explainValidateQuery() throws Exception {
        createIndex("test");
        ensureGreen();
        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("foo").field("type", "string").endObject()
                        .startObject("bar").field("type", "integer").endObject()
                        .startObject("baz").field("type", "string").field("analyzer", "snowball").endObject()
                        .startObject("pin").startObject("properties").startObject("location").field("type", "geo_point").endObject().endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child-type")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("child-type")
                        .startObject("_parent").field("type", "type1").endObject()
                        .startObject("properties")
                        .startObject("foo").field("type", "string").endObject()
                        .endObject()
                        .endObject().endObject())
                .execute().actionGet();

        refresh();

        ValidateQueryResponse response;
        response = client().admin().indices().prepareValidateQuery("test")
                .setSource("foo".getBytes(Charsets.UTF_8))
                .setExplain(true)
                .execute().actionGet();
        assertThat(response.isValid(), equalTo(false));
        assertThat(response.getQueryExplanation().size(), equalTo(1));
        assertThat(response.getQueryExplanation().get(0).getError(), containsString("Failed to parse"));
        assertThat(response.getQueryExplanation().get(0).getExplanation(), nullValue());

        final String typeFilter = filter("_type:type1");
        assertExplanation(QueryBuilders.queryString("_id:1"), equalTo("filtered(ConstantScore(_uid:type1#1))->" + typeFilter));

        assertExplanation(QueryBuilders.idsQuery("type1").addIds("1").addIds("2"),
                equalTo("filtered(ConstantScore(_uid:type1#1 _uid:type1#2))->" + typeFilter));

        assertExplanation(QueryBuilders.queryString("foo"), equalTo("filtered(_all:foo)->" + typeFilter));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.termQuery("foo", "1"),
                FilterBuilders.orFilter(
                        FilterBuilders.termFilter("bar", "2"),
                        FilterBuilders.termFilter("baz", "3")
                )
        ), equalTo("filtered(filtered(foo:1)->" + filter("bar:[2 TO 2]") + " " + filter("baz:3") + ")->" + typeFilter));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.termQuery("foo", "1"),
                FilterBuilders.orFilter(
                        FilterBuilders.termFilter("bar", "2")
                )
        ), equalTo("filtered(filtered(foo:1)->" + filter("bar:[2 TO 2]") + ")->" + typeFilter));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.matchAllQuery(),
                FilterBuilders.geoPolygonFilter("pin.location")
                        .addPoint(40, -70)
                        .addPoint(30, -80)
                        .addPoint(20, -90)
                        .addPoint(40, -70)    // closing polygon
        ), equalTo("filtered(ConstantScore(GeoPolygonFilter(pin.location, [[40.0, -70.0], [30.0, -80.0], [20.0, -90.0], [40.0, -70.0]])))->" + typeFilter));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.geoBoundingBoxFilter("pin.location")
                .topLeft(40, -80)
                .bottomRight(20, -70)
        ), equalTo("filtered(ConstantScore(GeoBoundingBoxFilter(pin.location, [40.0, -80.0], [20.0, -70.0])))->" + typeFilter));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.geoDistanceFilter("pin.location")
                .lat(10).lon(20).distance(15, DistanceUnit.DEFAULT).geoDistance(GeoDistance.PLANE)
        ), equalTo("filtered(ConstantScore(GeoDistanceFilter(pin.location, PLANE, 15.0, 10.0, 20.0)))->" + typeFilter));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.geoDistanceFilter("pin.location")
                .lat(10).lon(20).distance(15, DistanceUnit.DEFAULT).geoDistance(GeoDistance.PLANE)
        ), equalTo("filtered(ConstantScore(GeoDistanceFilter(pin.location, PLANE, 15.0, 10.0, 20.0)))->" + typeFilter));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.geoDistanceRangeFilter("pin.location")
                .lat(10).lon(20).from("15m").to("25m").geoDistance(GeoDistance.PLANE)
        ), equalTo("filtered(ConstantScore(GeoDistanceRangeFilter(pin.location, PLANE, [15.0 - 25.0], 10.0, 20.0)))->" + typeFilter));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.geoDistanceRangeFilter("pin.location")
                .lat(10).lon(20).from("15miles").to("25miles").geoDistance(GeoDistance.PLANE)
        ), equalTo("filtered(ConstantScore(GeoDistanceRangeFilter(pin.location, PLANE, [" + DistanceUnit.DEFAULT.convert(15.0, DistanceUnit.MILES) + " - " + DistanceUnit.DEFAULT.convert(25.0, DistanceUnit.MILES) + "], 10.0, 20.0)))->" + typeFilter));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.termQuery("foo", "1"),
                FilterBuilders.andFilter(
                        FilterBuilders.termFilter("bar", "2"),
                        FilterBuilders.termFilter("baz", "3")
                )
        ), equalTo("filtered(filtered(foo:1)->+" + filter("bar:[2 TO 2]") + " +" + filter("baz:3") + ")->" + typeFilter));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.termsFilter("foo", "1", "2", "3")),
                equalTo("filtered(ConstantScore(" + filter("foo:1 foo:2 foo:3") + "))->" + typeFilter));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.notFilter(FilterBuilders.termFilter("foo", "bar"))),
                equalTo("filtered(ConstantScore(NotFilter(" + filter("foo:bar") + ")))->" + typeFilter));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.termQuery("foo", "1"),
                FilterBuilders.hasChildFilter(
                        "child-type",
                        QueryBuilders.matchQuery("foo", "1")
                )
        ), equalTo("filtered(filtered(foo:1)->CustomQueryWrappingFilter(child_filter[child-type/type1](filtered(foo:1)->" + filter("_type:child-type") + ")))->" + typeFilter));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.termQuery("foo", "1"),
                FilterBuilders.scriptFilter("true")
        ), equalTo("filtered(filtered(foo:1)->ScriptFilter(true))->" + typeFilter));

    }

    @Test
    public void explainValidateQueryTwoNodes() throws IOException {
        createIndex("test");
        ensureGreen();
        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("foo").field("type", "string").endObject()
                        .startObject("bar").field("type", "integer").endObject()
                        .startObject("baz").field("type", "string").field("analyzer", "snowball").endObject()
                        .startObject("pin").startObject("properties").startObject("location").field("type", "geo_point").endObject().endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        refresh();

        for (Client client : internalCluster()) {
            ValidateQueryResponse response = client.admin().indices().prepareValidateQuery("test")
                    .setSource("foo".getBytes(Charsets.UTF_8))
                    .setExplain(true)
                    .execute().actionGet();
            assertThat(response.isValid(), equalTo(false));
            assertThat(response.getQueryExplanation().size(), equalTo(1));
            assertThat(response.getQueryExplanation().get(0).getError(), containsString("Failed to parse"));
            assertThat(response.getQueryExplanation().get(0).getExplanation(), nullValue());

        }

        for (Client client : internalCluster()) {
                ValidateQueryResponse response = client.admin().indices().prepareValidateQuery("test")
                    .setQuery(QueryBuilders.queryString("foo"))
                    .setExplain(true)
                    .execute().actionGet();
            assertThat(response.isValid(), equalTo(true));
            assertThat(response.getQueryExplanation().size(), equalTo(1));
            assertThat(response.getQueryExplanation().get(0).getExplanation(), equalTo("_all:foo"));
            assertThat(response.getQueryExplanation().get(0).getError(), nullValue());
        }
    }

    @Test //https://github.com/elasticsearch/elasticsearch/issues/3629
    public void explainDateRangeInQueryString() {
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()
                .put(indexSettings())
                .put("index.number_of_shards", 1)));

        String aMonthAgo = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).minusMonths(1));
        String aMonthFromNow = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).plusMonths(1));

        client().prepareIndex("test", "type", "1").setSource("past", aMonthAgo, "future", aMonthFromNow).get();

        refresh();

        ValidateQueryResponse response = client().admin().indices().prepareValidateQuery()
                .setQuery(queryString("past:[now-2M/d TO now/d]")).setExplain(true).get();

        assertNoFailures(response);
        assertThat(response.getQueryExplanation().size(), equalTo(1));
        assertThat(response.getQueryExplanation().get(0).getError(), nullValue());
        DateTime twoMonthsAgo = new DateTime(DateTimeZone.UTC).minusMonths(2).withTimeAtStartOfDay();
        DateTime now = new DateTime(DateTimeZone.UTC).plusDays(1).withTimeAtStartOfDay();
        assertThat(response.getQueryExplanation().get(0).getExplanation(),
                equalTo("past:[" + twoMonthsAgo.getMillis() + " TO " + now.getMillis() + "]"));
        assertThat(response.isValid(), equalTo(true));
    }

    @Test(expected = IndexMissingException.class)
    public void validateEmptyCluster() {
        client().admin().indices().prepareValidateQuery().get();
    }

    @Test
    public void explainNoQuery() {
        createIndex("test");
        ensureGreen();

        ValidateQueryResponse validateQueryResponse = client().admin().indices().prepareValidateQuery().setExplain(true).get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getIndex(), equalTo("test"));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), equalTo("ConstantScore(*:*)"));
    }

    @Test
    public void explainFilteredAlias() {
        assertAcked(prepareCreate("test")
                .addMapping("test", "field", "type=string")
                .addAlias(new Alias("alias").filter(FilterBuilders.termFilter("field", "value1"))));
        ensureGreen();

        ValidateQueryResponse validateQueryResponse = client().admin().indices().prepareValidateQuery("alias")
                .setQuery(QueryBuilders.matchAllQuery()).setExplain(true).get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getIndex(), equalTo("test"));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), containsString("field:value1"));
    }

    @Test
    public void explainMatchPhrasePrefix() {
        assertAcked(prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder().put(indexSettings())
                        .put("index.analysis.filter.syns.type", "synonym")
                        .putArray("index.analysis.filter.syns.synonyms", "one,two")
                        .put("index.analysis.analyzer.syns.tokenizer", "standard")
                        .putArray("index.analysis.analyzer.syns.filter", "syns")
                    ).addMapping("test", "field","type=string,analyzer=syns"));
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

    @Test
    public void irrelevantPropertiesBeforeQuery() throws IOException {
        createIndex("test");
        ensureGreen();
        refresh();

        assertThat(client().admin().indices().prepareValidateQuery("test").setSource(new BytesArray("{\"foo\": \"bar\", \"query\": {\"term\" : { \"user\" : \"kimchy\" }}}")).get().isValid(), equalTo(false));
    }

    @Test
    public void irrelevantPropertiesAfterQuery() throws IOException {
        createIndex("test");
        ensureGreen();
        refresh();

        assertThat(client().admin().indices().prepareValidateQuery("test").setSource(new BytesArray("{\"query\": {\"term\" : { \"user\" : \"kimchy\" }}, \"foo\": \"bar\"}")).get().isValid(), equalTo(false));
    }

    private void assertExplanation(QueryBuilder queryBuilder, Matcher<String> matcher) {
        ValidateQueryResponse response = client().admin().indices().prepareValidateQuery("test")
                .setTypes("type1")
                .setQuery(queryBuilder)
                .setExplain(true)
                .execute().actionGet();
        assertThat(response.getQueryExplanation().size(), equalTo(1));
        assertThat(response.getQueryExplanation().get(0).getError(), nullValue());
        assertThat(response.getQueryExplanation().get(0).getExplanation(), matcher);
        assertThat(response.isValid(), equalTo(true));
    }
}
