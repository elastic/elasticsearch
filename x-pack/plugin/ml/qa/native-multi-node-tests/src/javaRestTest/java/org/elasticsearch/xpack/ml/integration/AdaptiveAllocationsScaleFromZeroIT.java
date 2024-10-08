/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class AdaptiveAllocationsScaleFromZeroIT extends PyTorchModelRestTestCase {

    @Before
    public void setShortScaleToZeroPeriod() throws IOException {
        logger.info("setting time");
        Request scaleToZeroTime = new Request("PUT", "_cluster/settings");
        scaleToZeroTime.setJsonEntity("""
            {
              "persistent": {
                "xpack.ml.adaptive_allocations_scale_to_zero": "2s"
              }
            }""");

        client().performRequest(scaleToZeroTime);
    }

    @SuppressWarnings("unchecked")
    public void testScaleFromZero() throws Exception {
        String modelId = "test_scale_from_zero";
        createPassThroughModel(modelId);
        putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
        putVocabulary(List.of("Auto", "scale", "and", "infer"), modelId);

        startDeployment(modelId, modelId, new AdaptiveAllocationsSettings(true, 0, 1));

        var responseMap = entityAsMap(getTrainedModelStats(modelId));
        List<Map<String, Object>> stats = (List<Map<String, Object>>) responseMap.get("trained_model_stats");
        String statusState = (String) XContentMapValues.extractValue("deployment_stats.allocation_status.state", stats.get(0));
        assertThat(responseMap.toString(), statusState, is(not(nullValue())));
        Integer count = (Integer) XContentMapValues.extractValue("deployment_stats.allocation_status.allocation_count", stats.get(0));
        assertThat(responseMap.toString(), count, is(1));

        // wait for scale down. The scaler service will check every 10 seconds
        assertBusy(() -> {
            var statsMap = entityAsMap(getTrainedModelStats(modelId));
            List<Map<String, Object>> innerStats = (List<Map<String, Object>>) statsMap.get("trained_model_stats");
            Integer innerCount = (Integer) XContentMapValues.extractValue(
                "deployment_stats.allocation_status.allocation_count",
                innerStats.get(0)
            );
            assertThat(statsMap.toString(), innerCount, is(0));
        }, 30, TimeUnit.SECONDS);

        // infer will scale up
        int inferenceCount = 10;
        var latch = new CountDownLatch(inferenceCount);
        for (int i = 0; i < inferenceCount; i++) {
            asyncInfer("Auto scale and infer", modelId, TimeValue.timeValueSeconds(5), new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception exception) {
                    latch.countDown();
                    fail(exception.getMessage());
                }
            });
        }

        latch.await();
    }

    // public void testMultipleDeploymentsWaiting() {
    //
    // }

}
