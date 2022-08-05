/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RegressionTests;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class DataFrameAnalyticsCRUDIT extends MlSingleNodeTestCase {

    private DataFrameAnalyticsConfigProvider configProvider;

    @Before
    public void createComponents() throws Exception {
        configProvider = new DataFrameAnalyticsConfigProvider(
            client(),
            xContentRegistry(),
            new DataFrameAnalyticsAuditor(client(), getInstanceFromNode(ClusterService.class)),
            getInstanceFromNode(ClusterService.class)
        );
        waitForMlTemplates();
    }

    public void testGet_ConfigDoesNotExist() throws InterruptedException {
        AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(actionListener -> configProvider.get("missing", actionListener), configHolder, exceptionHolder);

        assertThat(configHolder.get(), is(nullValue()));
        assertThat(exceptionHolder.get(), is(notNullValue()));
        assertThat(exceptionHolder.get(), is(instanceOf(ResourceNotFoundException.class)));
    }

    public void testDeleteConfigWithStateAndStats() throws InterruptedException {
        String configId = "delete-config-with-state-and-stats";
        // Create valid config
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfigTests.createRandomBuilder(configId)
            .setAnalysis(RegressionTests.createRandom())
            .build();
        AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(
            actionListener -> configProvider.put(config, emptyMap(), TimeValue.timeValueSeconds(5), actionListener),
            configHolder,
            exceptionHolder
        );
        assertThat(configHolder.get(), is(notNullValue()));
        assertThat(configHolder.get(), is(equalTo(config)));

        OriginSettingClient originSettingClient = new OriginSettingClient(client(), ClientHelper.ML_ORIGIN);
        originSettingClient.prepareIndex(".ml-state-000001")
            .setId("delete-config-with-state-and-stats_regression_state#1")
            .setSource("{}", XContentType.JSON)
            .get();
        originSettingClient.prepareIndex(".ml-state-000001")
            .setId("data_frame_analytics-delete-config-with-state-and-stats-progress")
            .setSource("{}", XContentType.JSON)
            .get();
        originSettingClient.prepareIndex(".ml-stats-000001")
            .setId("delete-config-with-state-and-stats_1")
            .setSource("{\"job_id\": \"delete-config-with-state-and-stats\"}", XContentType.JSON)
            .get();
        originSettingClient.prepareIndex(".ml-stats-000001")
            .setId("delete-config-with-state-and-stats_2")
            .setSource("{\"job_id\": \"delete-config-with-state-and-stats\"}", XContentType.JSON)
            .get();

        originSettingClient.admin().indices().prepareRefresh(".ml-stat*").get();

        client().execute(DeleteDataFrameAnalyticsAction.INSTANCE, new DeleteDataFrameAnalyticsAction.Request(configId)).actionGet();

        assertThat(
            originSettingClient.prepareSearch(".ml-state-*")
                .setQuery(
                    QueryBuilders.idsQuery()
                        .addIds(
                            "delete-config-with-state-and-stats_regression_state#1",
                            "data_frame_analytics-delete-config-with-state-and-stats-progress"
                        )
                )
                .setTrackTotalHits(true)
                .get()
                .getHits()
                .getTotalHits().value,
            equalTo(0L)
        );

        assertThat(
            originSettingClient.prepareSearch(".ml-stats-*")
                .setQuery(QueryBuilders.idsQuery().addIds("delete-config-with-state-and-stats_1", "delete-config-with-state-and-stats_2"))
                .setTrackTotalHits(true)
                .get()
                .getHits()
                .getTotalHits().value,
            equalTo(0L)
        );
    }

}
