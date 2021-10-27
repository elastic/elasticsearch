/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DateMathIndexExpressionsIntegrationIT extends ESIntegTestCase {

    public void testIndexNameDateMathExpressions() {
        DateTime now = new DateTime(DateTimeZone.UTC);
        String index1 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now);
        String index2 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(2));
        createIndex(index1, index2, index3);

        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(index1, index2, index3).get();
        assertEquals(index1, getSettingsResponse.getSetting(index1, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(index2, getSettingsResponse.getSetting(index2, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(index3, getSettingsResponse.getSetting(index3, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        client().prepareIndex(dateMathExp1, "type", "1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex(dateMathExp2, "type", "2").setSource("{}", XContentType.JSON).get();
        client().prepareIndex(dateMathExp3, "type", "3").setSource("{}", XContentType.JSON).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch(dateMathExp1, dateMathExp2, dateMathExp3).get();
        assertHitCount(searchResponse, 3);
        assertSearchHits(searchResponse, "1", "2", "3");

        GetResponse getResponse = client().prepareGet(dateMathExp1, "type", "1").get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getId(), equalTo("1"));

        getResponse = client().prepareGet(dateMathExp2, "type", "2").get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getId(), equalTo("2"));

        getResponse = client().prepareGet(dateMathExp3, "type", "3").get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getId(), equalTo("3"));

        MultiGetResponse mgetResponse = client().prepareMultiGet()
            .add(dateMathExp1, "type", "1")
            .add(dateMathExp2, "type", "2")
            .add(dateMathExp3, "type", "3")
            .get();
        assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[0].getResponse().getId(), equalTo("1"));
        assertThat(mgetResponse.getResponses()[1].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[1].getResponse().getId(), equalTo("2"));
        assertThat(mgetResponse.getResponses()[2].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[2].getResponse().getId(), equalTo("3"));

        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(dateMathExp1, dateMathExp2, dateMathExp3).get();
        assertThat(indicesStatsResponse.getIndex(index1), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index2), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index3), notNullValue());

        DeleteResponse deleteResponse = client().prepareDelete(dateMathExp1, "type", "1").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getId(), equalTo("1"));

        deleteResponse = client().prepareDelete(dateMathExp2, "type", "2").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getId(), equalTo("2"));

        deleteResponse = client().prepareDelete(dateMathExp3, "type", "3").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getId(), equalTo("3"));
    }

    public void testAutoCreateIndexWithDateMathExpression() throws Exception {
        DateTime now = new DateTime(DateTimeZone.UTC);
        String index1 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now);
        String index2 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(2));

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        client().prepareIndex(dateMathExp1, "type", "1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex(dateMathExp2, "type", "2").setSource("{}", XContentType.JSON).get();
        client().prepareIndex(dateMathExp3, "type", "3").setSource("{}", XContentType.JSON).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch(dateMathExp1, dateMathExp2, dateMathExp3).get();
        assertHitCount(searchResponse, 3);
        assertSearchHits(searchResponse, "1", "2", "3");

        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(dateMathExp1, dateMathExp2, dateMathExp3).get();
        assertThat(indicesStatsResponse.getIndex(index1), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index2), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index3), notNullValue());
    }

    public void testCreateIndexWithDateMathExpression() throws Exception {
        DateTime now = new DateTime(DateTimeZone.UTC);
        String index1 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now);
        String index2 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(2));

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        createIndex(dateMathExp1, dateMathExp2, dateMathExp3);

        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(index1, index2, index3).get();
        assertEquals(dateMathExp1, getSettingsResponse.getSetting(index1, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(dateMathExp2, getSettingsResponse.getSetting(index2, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(dateMathExp3, getSettingsResponse.getSetting(index3, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metadata().index(index1), notNullValue());
        assertThat(clusterState.metadata().index(index2), notNullValue());
        assertThat(clusterState.metadata().index(index3), notNullValue());
    }

}
