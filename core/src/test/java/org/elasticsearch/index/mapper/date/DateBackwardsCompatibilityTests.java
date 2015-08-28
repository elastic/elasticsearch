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

package org.elasticsearch.index.mapper.date;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.hamcrest.Matchers.is;

/**
 * Test class to check for all the conditions defined in
 * https://github.com/elastic/elasticsearch/issues/10971
 */
public class DateBackwardsCompatibilityTests extends ESSingleNodeTestCase {

    private String index = "testindex";
    private String type = "testtype";
    private Version randomVersionBelow2x;

    @Before
    public void setup() throws Exception {
        randomVersionBelow2x = randomVersionBetween(getRandom(), Version.V_0_90_0, Version.V_1_6_1);
    }

    public void testThatPre2xIndicesNumbersAreTreatedAsEpochs() throws Exception {
        index = createPre2xIndexAndMapping();
        long dateInMillis = 1435073872l * 1000; // Tue Jun 23 17:37:52 CEST 2015
        XContentBuilder document = jsonBuilder().startObject().field("date_field", dateInMillis).endObject();
        index(document);

        // search for date in time range
        QueryBuilder query = QueryBuilders.rangeQuery("date_field").from("2015-06-23").to("2015-06-24");
        SearchResponse response = client().prepareSearch(index).setQuery(query).get();
        assertHitCount(response, 1);
    }

    public void testThatPre2xFailedStringParsingLeadsToEpochParsing() throws Exception {
        index = createPre2xIndexAndMapping();
        long dateInMillis = 1435073872l * 1000; // Tue Jun 23 17:37:52 CEST 2015
        String date = String.valueOf(dateInMillis);
        XContentBuilder document = jsonBuilder().startObject().field("date_field", date).endObject();
        index(document);

        // search for date in time range
        QueryBuilder query = QueryBuilders.rangeQuery("date_field").from("2015-06-23").to("2015-06-24");
        SearchResponse response = client().prepareSearch(index).setQuery(query).get();
        assertHitCount(response, 1);
    }

    public void testThatPre2xSupportsUnixTimestampsInAnyDateFormat() throws Exception {
        long dateInMillis = 1435073872l * 1000; // Tue Jun 23 17:37:52 CEST 2015
        List<String> dateFormats = Arrays.asList("dateOptionalTime", "weekDate", "tTime", "ordinalDate", "hourMinuteSecond", "hourMinute");

        for (String format : dateFormats) {
            XContentBuilder mapping = jsonBuilder().startObject().startObject("properties")
                    .startObject("date_field").field("type", "date").field("format", format).endObject()
                    .endObject().endObject();

            index = createIndex(randomVersionBelow2x, mapping);

            XContentBuilder document = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("date_field", String.valueOf(dateInMillis))
                    .endObject();
            index(document);

            // indexing as regular timestamp should work as well
            document = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("date_field", dateInMillis)
                    .endObject();
            index(document);

            client().admin().indices().prepareDelete(index).get();
        }
    }

    public void testThatPre2xIndicesNumbersAreTreatedAsTimestamps() throws Exception {
        // looks like a unix time stamp but is meant as 2016-06-23T01:00:00.000 - see the specified date format
        long date = 2015062301000l;

        XContentBuilder mapping = jsonBuilder().startObject().startObject("properties")
                .startObject("date_field").field("type", "date").field("format","yyyyMMddHHSSS").endObject()
                .endObject().endObject();
        index = createIndex(randomVersionBelow2x, mapping);

        XContentBuilder document = XContentFactory.jsonBuilder()
                .startObject()
                .field("date_field", randomBoolean() ? String.valueOf(date) : date)
                .endObject();
        index(document);

        // no results in expected time range
        QueryBuilder query = QueryBuilders.rangeQuery("date_field").from("2015-06-23").to("2015-06-24").format("dateOptionalTime");
        SearchResponse response = client().prepareSearch(index).setQuery(query).get();
        assertNoSearchHits(response);

        // result in unix timestamp range
        QueryBuilder timestampQuery = QueryBuilders.rangeQuery("date_field").from(2015062300000L).to(2015062302000L);
        assertHitCount(client().prepareSearch(index).setQuery(timestampQuery).get(), 1);

        // result should also work with regular specified dates
        QueryBuilder regularTimeQuery = QueryBuilders.rangeQuery("date_field").from("2033-11-08").to("2033-11-09").format("dateOptionalTime");
        assertHitCount(client().prepareSearch(index).setQuery(regularTimeQuery).get(), 1);
    }

    public void testThatPost2xIndicesNumbersAreTreatedAsStrings() throws Exception {
        // looks like a unix time stamp but is meant as 2016-06-23T01:00:00.000 - see the specified date format
        long date = 2015062301000l;

        XContentBuilder mapping = jsonBuilder().startObject().startObject("properties")
                .startObject("date_field").field("type", "date").field("format","yyyyMMddHHSSS").endObject()
                .endObject().endObject();
        index = createIndex(Version.CURRENT, mapping);

        XContentBuilder document = XContentFactory.jsonBuilder()
                .startObject()
                .field("date_field", String.valueOf(date))
                .endObject();
        index(document);

        document = XContentFactory.jsonBuilder()
                .startObject()
                .field("date_field", date)
                .endObject();
        index(document);

        // search for date in time range
        QueryBuilder query = QueryBuilders.rangeQuery("date_field").from("2015-06-23").to("2015-06-24").format("dateOptionalTime");
        SearchResponse response = client().prepareSearch(index).setQuery(query).get();
        assertHitCount(response, 2);
    }

    public void testDynamicDateDetectionIn2xDoesNotSupportEpochs() throws Exception {
        try {
            XContentBuilder mapping = jsonBuilder().startObject()
                    .startArray("dynamic_date_formats").value("dateOptionalTime").value("epoch_seconds").endArray()
                    .endObject();
            createIndex(Version.CURRENT, mapping);
            fail("Expected a MapperParsingException, but did not happen");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), is("mapping [" + type + "]"));
        }
    }

    private String createPre2xIndexAndMapping() throws Exception {
        return createIndexAndMapping(randomVersionBelow2x);
    }

    private String createIndexAndMapping(Version version) throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("properties")
                .startObject("date_field").field("type", "date").field("format", "dateOptionalTime").endObject()
                .endObject().endObject();

        return createIndex(version, mapping);
    }

    private String createIndex(Version version, XContentBuilder mapping) {
        Settings settings = settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        createIndex(index, settings, type, mapping);

        ensureGreen(index);
        return index;
    }

    private void index(XContentBuilder document) {
        IndexResponse indexResponse = client().prepareIndex(index, type).setSource(document).setRefresh(true).get();
        assertThat(indexResponse.isCreated(), is(true));
    }
}
