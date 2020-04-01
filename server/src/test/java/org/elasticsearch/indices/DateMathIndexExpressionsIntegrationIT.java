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

package org.elasticsearch.indices;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DateMathIndexExpressionsIntegrationIT extends ESIntegTestCase {
    private ZonedDateTime now;

    @Before
    public void setNow() {
        now = ZonedDateTime.now(ZoneOffset.UTC);
    }

    /**
     * the internal cluster calls System.nanoTime() and System.currentTimeMillis() during evaluations of requests
     * that need date-math index resolution. These are not mockable in these tests. As is, executing requests as-is
     * in these test cases can potentially result in invalid responses when day-boundaries are hit mid test run. Instead
     * of failing when index resolution with `now` is one day off, this method wraps calls with the assumption that
     * the day did not change during the test run.
     */
    public <Q extends ActionRequest, R extends ActionResponse> R dateSensitiveGet(ActionRequestBuilder<Q, R> builder) {
        Runnable dayChangeAssumption = () -> assumeTrue("day changed between requests",
            ZonedDateTime.now(ZoneOffset.UTC).getDayOfYear() == now.getDayOfYear());
        R response;
        try {
            response = builder.get();
        } catch (IndexNotFoundException e) {
            // index resolver throws this if it does not find the exact index due to day changes
            dayChangeAssumption.run();
            throw e;
        }
        dayChangeAssumption.run();
        return response;
    }

    public void testIndexNameDateMathExpressions() {
        String index1 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now);
        String index2 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(2));
        createIndex(index1, index2, index3);

        GetSettingsResponse getSettingsResponse = dateSensitiveGet(client().admin().indices().prepareGetSettings(index1, index2, index3));
        assertEquals(index1, getSettingsResponse.getSetting(index1, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(index2, getSettingsResponse.getSetting(index2, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(index3, getSettingsResponse.getSetting(index3, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));


        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        client().prepareIndex(dateMathExp1).setId("1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex(dateMathExp2).setId("2").setSource("{}", XContentType.JSON).get();
        client().prepareIndex(dateMathExp3).setId("3").setSource("{}", XContentType.JSON).get();
        refresh();

        SearchResponse searchResponse = dateSensitiveGet(client().prepareSearch(dateMathExp1, dateMathExp2, dateMathExp3));
        assertHitCount(searchResponse, 3);
        assertSearchHits(searchResponse, "1", "2", "3");

        GetResponse getResponse = dateSensitiveGet(client().prepareGet(dateMathExp1, "1"));
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getId(), equalTo("1"));

        getResponse = dateSensitiveGet(client().prepareGet(dateMathExp2, "2"));
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getId(), equalTo("2"));

        getResponse = dateSensitiveGet(client().prepareGet(dateMathExp3, "3"));
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getId(), equalTo("3"));

        MultiGetResponse mgetResponse = dateSensitiveGet(client().prepareMultiGet()
            .add(dateMathExp1, "1")
            .add(dateMathExp2, "2")
            .add(dateMathExp3, "3"));
        assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[0].getResponse().getId(), equalTo("1"));
        assertThat(mgetResponse.getResponses()[1].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[1].getResponse().getId(), equalTo("2"));
        assertThat(mgetResponse.getResponses()[2].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[2].getResponse().getId(), equalTo("3"));

        IndicesStatsResponse indicesStatsResponse =
            dateSensitiveGet(client().admin().indices().prepareStats(dateMathExp1, dateMathExp2, dateMathExp3));
        assertThat(indicesStatsResponse.getIndex(index1), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index2), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index3), notNullValue());

        DeleteResponse deleteResponse = dateSensitiveGet(client().prepareDelete(dateMathExp1, "1"));
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getId(), equalTo("1"));

        deleteResponse = dateSensitiveGet(client().prepareDelete(dateMathExp2, "2"));
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getId(), equalTo("2"));

        deleteResponse = dateSensitiveGet(client().prepareDelete(dateMathExp3, "3"));
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getId(), equalTo("3"));
    }

    public void testAutoCreateIndexWithDateMathExpression() {
        String index1 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now);
        String index2 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(2));

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        client().prepareIndex(dateMathExp1).setId("1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex(dateMathExp2).setId("2").setSource("{}", XContentType.JSON).get();
        client().prepareIndex(dateMathExp3).setId("3").setSource("{}", XContentType.JSON).get();
        refresh();

        SearchResponse searchResponse = dateSensitiveGet(client().prepareSearch(dateMathExp1, dateMathExp2, dateMathExp3));
        assertHitCount(searchResponse, 3);
        assertSearchHits(searchResponse, "1", "2", "3");

        IndicesStatsResponse indicesStatsResponse =
            dateSensitiveGet(client().admin().indices().prepareStats(dateMathExp1, dateMathExp2, dateMathExp3));
        assertThat(indicesStatsResponse.getIndex(index1), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index2), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index3), notNullValue());
    }

    public void testCreateIndexWithDateMathExpression() {
        String index1 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now);
        String index2 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(2));

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        createIndex(dateMathExp1, dateMathExp2, dateMathExp3);

        GetSettingsResponse getSettingsResponse = dateSensitiveGet(client().admin().indices().prepareGetSettings(index1, index2, index3));
        assertEquals(dateMathExp1, getSettingsResponse.getSetting(index1, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(dateMathExp2, getSettingsResponse.getSetting(index2, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(dateMathExp3, getSettingsResponse.getSetting(index3, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metadata().index(index1), notNullValue());
        assertThat(clusterState.metadata().index(index2), notNullValue());
        assertThat(clusterState.metadata().index(index3), notNullValue());
    }

}
