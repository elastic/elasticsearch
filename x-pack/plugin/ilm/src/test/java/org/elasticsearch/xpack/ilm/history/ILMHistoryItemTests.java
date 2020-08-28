/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.history;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class ILMHistoryItemTests extends ESTestCase {

    public void testToXContent() throws IOException {
        ILMHistoryItem success = ILMHistoryItem.success("index", "policy", 1234L, 100L,
            LifecycleExecutionState.builder()
                .setPhase("phase")
                .setAction("action")
                .setStep("step")
                .setPhaseTime(10L)
                .setActionTime(20L)
                .setStepTime(30L)
                .setPhaseDefinition("{}")
                .setStepInfo("{\"step_info\": \"foo\"")
                .build());

        ILMHistoryItem failure = ILMHistoryItem.failure("index", "policy", 1234L, 100L,
            LifecycleExecutionState.builder()
                .setPhase("phase")
                .setAction("action")
                .setStep("ERROR")
                .setFailedStep("step")
                .setFailedStepRetryCount(7)
                .setIsAutoRetryableError(true)
                .setPhaseTime(10L)
                .setActionTime(20L)
                .setStepTime(30L)
                .setPhaseDefinition("{\"phase_json\": \"eggplant\"}")
                .setStepInfo("{\"step_info\": \"foo\"")
                .build(),
            new IllegalArgumentException("failure"));

        try (XContentBuilder builder = jsonBuilder()) {
            success.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = Strings.toString(builder);
            assertThat(json, equalTo("{\"index\":\"index\"," +
                "\"policy\":\"policy\"," +
                "\"@timestamp\":1234," +
                "\"index_age\":100," +
                "\"success\":true," +
                "\"state\":{\"phase\":\"phase\"," +
                "\"phase_definition\":\"{}\"," +
                "\"action_time\":\"20\"," +
                "\"phase_time\":\"10\"," +
                "\"step_info\":\"{\\\"step_info\\\": \\\"foo\\\"\",\"action\":\"action\",\"step\":\"step\",\"step_time\":\"30\"}}"
            ));
        }

        try (XContentBuilder builder = jsonBuilder()) {
            failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = Strings.toString(builder);
            assertThat(json, startsWith("{\"index\":\"index\"," +
                "\"policy\":\"policy\"," +
                "\"@timestamp\":1234," +
                "\"index_age\":100," +
                "\"success\":false," +
                "\"state\":{\"phase\":\"phase\"," +
                "\"failed_step\":\"step\"," +
                "\"phase_definition\":\"{\\\"phase_json\\\": \\\"eggplant\\\"}\"," +
                "\"action_time\":\"20\"," +
                "\"is_auto_retryable_error\":\"true\"," +
                "\"failed_step_retry_count\":\"7\"," +
                "\"phase_time\":\"10\"," +
                "\"step_info\":\"{\\\"step_info\\\": \\\"foo\\\"\"," +
                "\"action\":\"action\"," +
                "\"step\":\"ERROR\"," +
                "\"step_time\":\"30\"}," +
                "\"error_details\":\"{\\\"type\\\":\\\"illegal_argument_exception\\\"," +
                "\\\"reason\\\":\\\"failure\\\"," +
                "\\\"stack_trace\\\":\\\"java.lang.IllegalArgumentException: failure"));
        }
    }
}
