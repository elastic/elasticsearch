/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.RequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
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
    public <Q extends ActionRequest, R extends ActionResponse> void dateSensitiveGet(RequestBuilder<Q, R> builder, Consumer<R> consumer) {
        Runnable dayChangeAssumption = () -> assumeTrue(
            "day changed between requests",
            ZonedDateTime.now(ZoneOffset.UTC).getDayOfYear() == now.getDayOfYear()
        );
        try {
            assertResponse(builder, response -> {
                dayChangeAssumption.run();
                consumer.accept(response);
            });
        } catch (IndexNotFoundException e) {
            // index resolver throws this if it does not find the exact index due to day changes
            dayChangeAssumption.run();
            throw e;
        }

    }

    public void testIndexNameDateMathExpressions() {
        String index1 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now);
        String index2 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(2));
        createIndex(index1, index2, index3);

        dateSensitiveGet(indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, index1, index2, index3), response -> {
            assertEquals(index1, response.getSetting(index1, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
            assertEquals(index2, response.getSetting(index2, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
            assertEquals(index3, response.getSetting(index3, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        });

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        prepareIndex(dateMathExp1).setId("1").setSource("{}", XContentType.JSON).get();
        prepareIndex(dateMathExp2).setId("2").setSource("{}", XContentType.JSON).get();
        prepareIndex(dateMathExp3).setId("3").setSource("{}", XContentType.JSON).get();
        refresh();

        dateSensitiveGet(prepareSearch(dateMathExp1, dateMathExp2, dateMathExp3), response -> {
            assertHitCount(response, 3);
            assertSearchHits(response, "1", "2", "3");
        });

        dateSensitiveGet(client().prepareGet(dateMathExp1, "1"), response -> {
            assertThat(response.isExists(), is(true));
            assertThat(response.getId(), equalTo("1"));
        });

        dateSensitiveGet(client().prepareGet(dateMathExp2, "2"), response -> {
            assertThat(response.isExists(), is(true));
            assertThat(response.getId(), equalTo("2"));
        });

        dateSensitiveGet(client().prepareGet(dateMathExp3, "3"), response -> {
            assertThat(response.isExists(), is(true));
            assertThat(response.getId(), equalTo("3"));
        });

        dateSensitiveGet(client().prepareMultiGet().add(dateMathExp1, "1").add(dateMathExp2, "2").add(dateMathExp3, "3"), response -> {
            assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
            assertThat(response.getResponses()[0].getResponse().getId(), equalTo("1"));
            assertThat(response.getResponses()[1].getResponse().isExists(), is(true));
            assertThat(response.getResponses()[1].getResponse().getId(), equalTo("2"));
            assertThat(response.getResponses()[2].getResponse().isExists(), is(true));
            assertThat(response.getResponses()[2].getResponse().getId(), equalTo("3"));
        });

        dateSensitiveGet(indicesAdmin().prepareStats(dateMathExp1, dateMathExp2, dateMathExp3), response -> {
            assertThat(response.getIndex(index1), notNullValue());
            assertThat(response.getIndex(index2), notNullValue());
            assertThat(response.getIndex(index3), notNullValue());
        });

        dateSensitiveGet(client().prepareDelete(dateMathExp1, "1"), response -> {
            assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
            assertThat(response.getId(), equalTo("1"));
        });

        dateSensitiveGet(client().prepareDelete(dateMathExp2, "2"), response -> {
            assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
            assertThat(response.getId(), equalTo("2"));
        });

        dateSensitiveGet(client().prepareDelete(dateMathExp3, "3"), response -> {
            assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
            assertThat(response.getId(), equalTo("3"));
        });
    }

    public void testAutoCreateIndexWithDateMathExpression() {
        String index1 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now);
        String index2 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(2));

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        prepareIndex(dateMathExp1).setId("1").setSource("{}", XContentType.JSON).get();
        prepareIndex(dateMathExp2).setId("2").setSource("{}", XContentType.JSON).get();
        prepareIndex(dateMathExp3).setId("3").setSource("{}", XContentType.JSON).get();
        refresh();

        dateSensitiveGet(prepareSearch(dateMathExp1, dateMathExp2, dateMathExp3), response -> {
            assertHitCount(response, 3);
            assertSearchHits(response, "1", "2", "3");
        });

        dateSensitiveGet(indicesAdmin().prepareStats(dateMathExp1, dateMathExp2, dateMathExp3), response -> {
            assertThat(response.getIndex(index1), notNullValue());
            assertThat(response.getIndex(index2), notNullValue());
            assertThat(response.getIndex(index3), notNullValue());
        });
    }

    public void testCreateIndexWithDateMathExpression() {
        String index1 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now);
        String index2 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).format(now.minusDays(2));

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        createIndex(dateMathExp1, dateMathExp2, dateMathExp3);

        dateSensitiveGet(indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, index1, index2, index3), response -> {
            assertEquals(dateMathExp1, response.getSetting(index1, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
            assertEquals(dateMathExp2, response.getSetting(index2, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
            assertEquals(dateMathExp3, response.getSetting(index3, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        });

        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        assertThat(clusterState.metadata().getProject().index(index1), notNullValue());
        assertThat(clusterState.metadata().getProject().index(index2), notNullValue());
        assertThat(clusterState.metadata().getProject().index(index3), notNullValue());
    }
}
