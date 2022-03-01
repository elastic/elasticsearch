/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class IndexLayoutIT extends BaseMlIntegTestCase {

    public void testCrudOnTwoJobsInSharedIndex() throws Exception {
        String jobId = "index-layout-job";
        String jobId2 = "index-layout-job2";

        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId, ByteSizeValue.ofMb(2)))).get();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId2, ByteSizeValue.ofMb(2)))).get();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).get();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId2)).get();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                .actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(jobId2)
            ).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });

        OriginSettingClient client = new OriginSettingClient(client(), ML_ORIGIN);
        assertThat(
            client.admin().indices().prepareGetIndex().addIndices(AnomalyDetectorsIndex.jobStateIndexPattern()).get().indices(),
            arrayContaining(".ml-state-000001")
        );
        assertThat(
            client.admin()
                .indices()
                .prepareGetAliases(AnomalyDetectorsIndex.jobStateIndexPattern())
                .get()
                .getAliases()
                .get(".ml-state-000001")
                .get(0)
                .alias(),
            equalTo(".ml-state-write")
        );
        assertThat(
            client.admin()
                .indices()
                .prepareGetIndex()
                .addIndices(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))
                .get()
                .indices().length,
            equalTo(1)
        );
        assertThat(
            client.admin()
                .indices()
                .prepareGetIndex()
                .addIndices(AnomalyDetectorsIndex.jobResultsAliasedName(jobId2))
                .get()
                .indices().length,
            equalTo(1)
        );
    }

    public void testForceCloseDoesNotCreateState() throws Exception {
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        String jobId = "index-layout-force-close-job";

        client().execute(
            PutJobAction.INSTANCE,
            new PutJobAction.Request(createJob(jobId, ByteSizeValue.ofMb(2)).setDataDescription(new DataDescription.Builder()))
        ).get();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).get();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                .actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        indexDocs(logger, "data", 100, weekAgo, now);
        client().execute(
            PutDatafeedAction.INSTANCE,
            new PutDatafeedAction.Request(createDatafeed(jobId + "-datafeed", jobId, Collections.singletonList("data")))
        ).get();

        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(jobId + "-datafeed", 0)).get();

        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                .actionGet();
            assertThat(statsResponse.getResponse().results().get(0).getDataCounts().getInputRecordCount(), greaterThan(0L));
        });

        client().execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(jobId + "-datafeed")).get();

        CloseJobAction.Request closeRequest = new CloseJobAction.Request(jobId);
        closeRequest.setForce(true);
        client().execute(CloseJobAction.INSTANCE, closeRequest).get();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                .actionGet();
            assertThat(statsResponse.getResponse().results().get(0).getState(), equalTo(JobState.CLOSED));
        });

        OriginSettingClient client = new OriginSettingClient(client(), ML_ORIGIN);
        assertThat(
            client.admin().indices().prepareGetIndex().addIndices(AnomalyDetectorsIndex.jobStateIndexPattern()).get().indices(),
            arrayContaining(".ml-state-000001")
        );

        assertThat(
            client.prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern()).setTrackTotalHits(true).get().getHits().getTotalHits().value,
            equalTo(0L)
        );
    }

}
