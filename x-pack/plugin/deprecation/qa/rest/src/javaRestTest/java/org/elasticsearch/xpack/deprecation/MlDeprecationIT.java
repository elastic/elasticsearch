/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.client.feature.ResetFeaturesRequest;
import org.elasticsearch.client.feature.ResetFeaturesResponse;
import org.elasticsearch.client.migration.DeprecationInfoRequest;
import org.elasticsearch.client.migration.DeprecationInfoResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.DataDescription;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MlDeprecationIT extends ESRestTestCase {

    private static final RequestOptions REQUEST_OPTIONS = RequestOptions.DEFAULT.toBuilder()
        .setWarningsHandler(WarningsHandler.PERMISSIVE)
        .build();

    private static class HLRC extends RestHighLevelClient {
        HLRC(RestClient restClient) {
            super(restClient, RestClient::close, new ArrayList<>());
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    /**
     * Currently the generic test cleanup code that runs in between tests is causing deprecation warnings
     * when it deletes system indices like .ml-config. This causes particular problems within this test
     * suite that's specifically testing for deprecations. Therefore, before the generic cleanup code runs,
     * ensure that all ML system indices are already deleted.
     *
     * TODO: delete this method once the generic cleanup code deletes system indices without generating
     *       deprecation warnings.
     */
    @After
    public void resetFeatures() throws Exception {
        HLRC adminHlrc = new HLRC(adminClient());
        List<ResetFeaturesResponse.ResetFeatureStateStatus> statuses = adminHlrc.features()
            .resetFeatures(new ResetFeaturesRequest(), RequestOptions.DEFAULT)
            .getFeatureResetStatuses();
        for (ResetFeaturesResponse.ResetFeatureStateStatus status : statuses) {
            if ("machine_learning".equals(status.getFeatureName()) && status.getException() != null) {
                throw status.getException();
            }
        }
        // It's deliberate not to close the admin client
    }

    public void testMlDeprecationChecks() throws Exception {
        HLRC hlrc = new HLRC(client());
        String jobId = "deprecation_check_job";
        hlrc.machineLearning()
            .putJob(
                new PutJobRequest(
                    Job.builder(jobId)
                        .setAnalysisConfig(
                            AnalysisConfig.builder(Collections.singletonList(Detector.builder().setFunction("count").build()))
                        )
                        .setDataDescription(new DataDescription.Builder().setTimeField("time"))
                        .build()
                ),
                REQUEST_OPTIONS
            );

        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-.write-" + jobId).id(jobId + "_model_snapshot_1")
            .source("{\"job_id\":\"deprecation_check_job\",\"snapshot_id\":\"1\", \"snapshot_doc_count\":1}", XContentType.JSON);
        hlrc.index(indexRequest, REQUEST_OPTIONS);

        indexRequest = new IndexRequest(".ml-anomalies-.write-" + jobId).id(jobId + "_model_snapshot_2")
            .source(
                "{\"job_id\":\"deprecation_check_job\",\"snapshot_id\":\"2\",\"snapshot_doc_count\":1,\"min_version\":\"8.0.0\"}",
                XContentType.JSON
            );
        hlrc.index(indexRequest, REQUEST_OPTIONS);
        hlrc.indices().refresh(new RefreshRequest(".ml-anomalies-*"), REQUEST_OPTIONS);

        DeprecationInfoResponse response = hlrc.migration()
            .getDeprecationInfo(
                // specify an index so that deprecation checks don't run against any accidentally existing indices
                new DeprecationInfoRequest(Collections.singletonList("index-that-does-not-exist-*")),
                RequestOptions.DEFAULT
            );
        assertThat(response.getMlSettingsIssues(), hasSize(1));
        assertThat(
            response.getMlSettingsIssues().get(0).getMessage(),
            containsString("model snapshot [1] for job [deprecation_check_job] needs to be deleted or upgraded")
        );
        assertThat(response.getMlSettingsIssues().get(0).getMeta(), equalTo(Map.of("job_id", jobId, "snapshot_id", "1")));
        hlrc.close();
    }
}
