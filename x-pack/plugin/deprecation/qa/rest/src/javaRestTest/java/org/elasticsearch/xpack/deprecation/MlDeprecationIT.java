/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.WarningsHandler;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@SuppressWarnings("removal")
public class MlDeprecationIT extends ESRestTestCase {

    private static final RequestOptions REQUEST_OPTIONS = RequestOptions.DEFAULT.toBuilder()
        .setWarningsHandler(WarningsHandler.PERMISSIVE)
        .build();

    private static class HLRC extends RestHighLevelClient {
        HLRC(RestClient restClient) {
            super(restClient, RestClient::close, new ArrayList<>());
        }
    }

    @After
    public void resetFeatures() throws IOException {
        Response response = adminClient().performRequest(new Request("POST", "/_features/_reset"));
        assertOK(response);
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

    @SuppressWarnings("unchecked")
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

        // specify an index so that deprecation checks don't run against any accidentally existing indices
        Request getDeprecations = new Request("GET", "/does-not-exist-*/_migration/deprecations");
        Map<String, Object> deprecationsResponse = responseAsMap(adminClient().performRequest(getDeprecations));
        List<Map<String, Object>> mlSettingsDeprecations = (List<Map<String, Object>>) deprecationsResponse.get("ml_settings");
        assertThat(mlSettingsDeprecations, hasSize(1));
        assertThat(
            (String) mlSettingsDeprecations.get(0).get("message"),
            containsString("Model snapshot [1] for job [deprecation_check_job] has an obsolete minimum version")
        );
        assertThat(mlSettingsDeprecations.get(0).get("_meta"), equalTo(Map.of("job_id", jobId, "snapshot_id", "1")));
    }

}
