/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleValidateQueryTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleValidateQuery() throws Exception {

        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("foo").field("type", "string").endObject()
                        .startObject("bar").field("type", "integer").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery("foo".getBytes(Charsets.UTF_8)).execute().actionGet().isValid(), equalTo(false));
        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("_id:1")).execute().actionGet().isValid(), equalTo(true));
        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("_i:d:1")).execute().actionGet().isValid(), equalTo(false));

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("foo:1")).execute().actionGet().isValid(), equalTo(true));
        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("bar:hey")).execute().actionGet().isValid(), equalTo(false));

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("nonexistent:hello")).execute().actionGet().isValid(), equalTo(true));

        assertThat(client().admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("foo:1 AND")).execute().actionGet().isValid(), equalTo(false));
    }

    @Test
    public void explainValidateQuery() throws Exception {

        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
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

        client().admin().indices().prepareRefresh().execute().actionGet();


        ValidateQueryResponse response;
        response = client().admin().indices().prepareValidateQuery("test")
                .setQuery("foo".getBytes(Charsets.UTF_8))
                .setExplain(true)
                .execute().actionGet();
        assertThat(response.isValid(), equalTo(false));
        assertThat(response.getQueryExplanation().size(), equalTo(1));
        assertThat(response.getQueryExplanation().get(0).getError(), containsString("Failed to parse"));
        assertThat(response.getQueryExplanation().get(0).getExplanation(), nullValue());

        assertExplanation(QueryBuilders.queryString("_id:1"), equalTo("ConstantScore(_uid:type1#1)"));

        assertExplanation(QueryBuilders.idsQuery("type1").addIds("1").addIds("2"),
                equalTo("ConstantScore(_uid:type1#1 _uid:type1#2)"));

        assertExplanation(QueryBuilders.queryString("foo"), equalTo("_all:foo"));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.termQuery("foo", "1"),
                FilterBuilders.orFilter(
                        FilterBuilders.termFilter("bar", "2"),
                        FilterBuilders.termFilter("baz", "3")
                )
        ), equalTo("filtered(foo:1)->cache(bar:[2 TO 2]) cache(baz:3)"));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.termQuery("foo", "1"),
                FilterBuilders.orFilter(
                        FilterBuilders.termFilter("bar", "2")
                )
        ), equalTo("filtered(foo:1)->cache(bar:[2 TO 2])"));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.matchAllQuery(),
                FilterBuilders.geoPolygonFilter("pin.location")
                        .addPoint(40, -70)
                        .addPoint(30, -80)
                        .addPoint(20, -90)
        ), equalTo("ConstantScore(GeoPolygonFilter(pin.location, [[40.0, -70.0], [30.0, -80.0], [20.0, -90.0]]))"));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.geoBoundingBoxFilter("pin.location")
                .topLeft(40, -80)
                .bottomRight(20, -70)
        ), equalTo("ConstantScore(GeoBoundingBoxFilter(pin.location, [40.0, -80.0], [20.0, -70.0]))"));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.geoDistanceFilter("pin.location")
                .lat(10).lon(20).distance(15, DistanceUnit.MILES).geoDistance(GeoDistance.PLANE)
        ), equalTo("ConstantScore(GeoDistanceFilter(pin.location, PLANE, 15.0, 10.0, 20.0))"));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.geoDistanceFilter("pin.location")
                .lat(10).lon(20).distance(15, DistanceUnit.MILES).geoDistance(GeoDistance.PLANE)
        ), equalTo("ConstantScore(GeoDistanceFilter(pin.location, PLANE, 15.0, 10.0, 20.0))"));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.geoDistanceRangeFilter("pin.location")
                .lat(10).lon(20).from("15miles").to("25miles").geoDistance(GeoDistance.PLANE)
        ), equalTo("ConstantScore(GeoDistanceRangeFilter(pin.location, PLANE, [15.0 - 25.0], 10.0, 20.0))"));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.termQuery("foo", "1"),
                FilterBuilders.andFilter(
                        FilterBuilders.termFilter("bar", "2"),
                        FilterBuilders.termFilter("baz", "3")
                )
        ), equalTo("filtered(foo:1)->+cache(bar:[2 TO 2]) +cache(baz:3)"));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.termsFilter("foo", "1", "2", "3")),
                equalTo("ConstantScore(cache(foo:1 foo:2 foo:3))"));

        assertExplanation(QueryBuilders.constantScoreQuery(FilterBuilders.notFilter(FilterBuilders.termFilter("foo", "bar"))),
                equalTo("ConstantScore(NotFilter(cache(foo:bar)))"));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.termQuery("foo", "1"),
                FilterBuilders.hasChildFilter(
                        "child-type",
                        QueryBuilders.fieldQuery("foo", "1")
                )
        ), equalTo("filtered(foo:1)->CustomQueryWrappingFilter(child_filter[child-type/type1](filtered(foo:1)->cache(_type:child-type)))"));

        assertExplanation(QueryBuilders.filteredQuery(
                QueryBuilders.termQuery("foo", "1"),
                FilterBuilders.scriptFilter("true")
        ), equalTo("filtered(foo:1)->ScriptFilter(true)"));

    }

    @Test
    public void explainValidateQueryTwoNodes() throws IOException {

        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("foo").field("type", "string").endObject()
                        .startObject("bar").field("type", "integer").endObject()
                        .startObject("baz").field("type", "string").field("analyzer", "snowball").endObject()
                        .startObject("pin").startObject("properties").startObject("location").field("type", "geo_point").endObject().endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();


        
        for (Client client : cluster()) {
            ValidateQueryResponse response = client.admin().indices().prepareValidateQuery("test")
                    .setQuery("foo".getBytes(Charsets.UTF_8))
                    .setExplain(true)
                    .execute().actionGet();
            assertThat(response.isValid(), equalTo(false));
            assertThat(response.getQueryExplanation().size(), equalTo(1));
            assertThat(response.getQueryExplanation().get(0).getError(), containsString("Failed to parse"));
            assertThat(response.getQueryExplanation().get(0).getExplanation(), nullValue());

        }
        
        for (Client client : cluster()) {
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
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).get();

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
