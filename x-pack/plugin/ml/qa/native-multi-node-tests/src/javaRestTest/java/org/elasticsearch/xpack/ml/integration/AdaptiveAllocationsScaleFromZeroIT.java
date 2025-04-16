/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@LuceneTestCase.AwaitsFix(bugUrl = "Cannot test without setting the scale to zero period to a small value")
public class AdaptiveAllocationsScaleFromZeroIT extends PyTorchModelRestTestCase {

    @SuppressWarnings("unchecked")
    public void testScaleFromZero() throws Exception {
        String modelId = "test_scale_from_zero";
        createPassThroughModel(modelId);
        putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
        putVocabulary(List.of("Auto", "scale", "and", "infer"), modelId);

        startDeployment(modelId, modelId, new AdaptiveAllocationsSettings(true, 0, 1));
        {
            var responseMap = entityAsMap(getTrainedModelStats(modelId));
            List<Map<String, Object>> stats = (List<Map<String, Object>>) responseMap.get("trained_model_stats");
            String statusState = (String) XContentMapValues.extractValue("deployment_stats.allocation_status.state", stats.get(0));
            assertThat(responseMap.toString(), statusState, is(not(nullValue())));
            Integer count = (Integer) XContentMapValues.extractValue("deployment_stats.allocation_status.allocation_count", stats.get(0));
            assertThat(responseMap.toString(), count, is(1));
        }

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

        var failures = new ConcurrentLinkedDeque<Exception>();

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
                    failures.add(exception);
                }
            });
        }

        latch.await();
        assertThat(failures, empty());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleDeploymentsWaiting() throws Exception {
        String id1 = "test_scale_from_zero_dep_1";
        String id2 = "test_scale_from_zero_dep_2";
        String id3 = "test_scale_from_zero_dep_3";
        var idsList = Arrays.asList(id1, id2, id3);
        for (var modelId : idsList) {
            createPassThroughModel(modelId);
            putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
            putVocabulary(List.of("Auto", "scale", "and", "infer"), modelId);

            startDeployment(modelId, modelId, new AdaptiveAllocationsSettings(true, 0, 1));
        }

        // wait for scale down. The scaler service will check every 10 seconds
        assertBusy(() -> {
            var statsMap = entityAsMap(getTrainedModelStats("test_scale_from_zero_dep_*"));
            List<Map<String, Object>> innerStats = (List<Map<String, Object>>) statsMap.get("trained_model_stats");
            assertThat(innerStats, hasSize(3));
            for (int i = 0; i < 3; i++) {
                Integer innerCount = (Integer) XContentMapValues.extractValue(
                    "deployment_stats.allocation_status.allocation_count",
                    innerStats.get(i)
                );
                assertThat(statsMap.toString(), innerCount, is(0));
            }
        }, 30, TimeUnit.SECONDS);

        // infer will scale up
        int inferenceCount = 10;
        var latch = new CountDownLatch(inferenceCount);
        for (int i = 0; i < inferenceCount; i++) {
            asyncInfer("Auto scale and infer", randomFrom(idsList), TimeValue.timeValueSeconds(5), new ResponseListener() {
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
}
