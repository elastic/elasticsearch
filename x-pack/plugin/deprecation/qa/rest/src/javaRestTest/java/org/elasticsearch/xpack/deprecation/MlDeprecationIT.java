/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;

import java.io.IOException;
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
        String jobId = "deprecation_check_job";
        buildAndPutJob(jobId);

        indexDoc(
            ".ml-anomalies-.write-" + jobId,
            jobId + "_model_snapshot_1",
            "{\"job_id\":\"deprecation_check_job\",\"snapshot_id\":\"1\", \"snapshot_doc_count\":1}"
        );

        indexDoc(
            ".ml-anomalies-.write-" + jobId,
            jobId + "_model_snapshot_2",
            "{\"job_id\":\"deprecation_check_job\",\"snapshot_id\":\"2\",\"snapshot_doc_count\":1,\"min_version\":\"8.3.0\"}"
        );
        client().performRequest(new Request("POST", "/.ml-anomalies-*/_refresh"));

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

    private Response buildAndPutJob(String jobId) throws Exception {
        String jobConfig = """
            {
                "analysis_config" : {
                    "bucket_span": "3600s",
                    "detectors" :[{"function":"count"}]
                },
                "data_description" : {
                    "time_field":"time",
                    "time_format":"yyyy-MM-dd HH:mm:ssX"
                }
            }""";

        Request request = new Request("PUT", "/_ml/anomaly_detectors/" + jobId);
        request.setOptions(REQUEST_OPTIONS);
        request.setJsonEntity(jobConfig);
        return client().performRequest(request);
    }

    private Response indexDoc(String index, String docId, String source) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + docId);
        request.setOptions(REQUEST_OPTIONS);
        request.setJsonEntity(source);
        return client().performRequest(request);
    }

}
