/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.history;

import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.startsWith;

public class ILMHistoryItemTests extends ESTestCase {

    public void testToXContent() throws IOException {
        ILMHistoryItem success = ILMHistoryItem.success(
            "index",
            "policy",
            1234L,
            100L,
            LifecycleExecutionState.builder()
                .setPhase("phase")
                .setAction("action")
                .setStep("step")
                .setPhaseTime(10L)
                .setActionTime(20L)
                .setStepTime(30L)
                .setPhaseDefinition("{}")
                .setStepInfo("{\"step_info\": \"foo\"")
                .build()
        );

        ILMHistoryItem failure = ILMHistoryItem.failure(
            "index",
            "policy",
            1234L,
            100L,
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
            new IllegalArgumentException("failure")
        );

        try (XContentBuilder builder = jsonBuilder()) {
            success.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = Strings.toString(builder);
            assertThat(json, equalTo(XContentHelper.stripWhitespace("""
                {
                  "index": "index",
                  "policy": "policy",
                  "@timestamp": 1234,
                  "index_age": 100,
                  "success": true,
                  "state": {
                    "phase": "phase",
                    "phase_definition": "{}",
                    "action_time": "20",
                    "phase_time": "10",
                    "step_info": "{\\"step_info\\": \\"foo\\"",
                    "action": "action",
                    "step": "step",
                    "step_time": "30"
                  }
                }""")));
        }

        try (XContentBuilder builder = jsonBuilder()) {
            failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = Strings.toString(builder);
            assertThat(json.replaceAll("\\s", ""), startsWith("""
                {
                  "index": "index",
                  "policy": "policy",
                  "@timestamp": 1234,
                  "index_age": 100,
                  "success": false,
                  "state": {
                    "phase": "phase",
                    "failed_step": "step",
                    "phase_definition": "{\\"phase_json\\": \\"eggplant\\"}",
                    "action_time": "20",
                    "is_auto_retryable_error": "true",
                    "failed_step_retry_count": "7",
                    "phase_time": "10",
                    "step_info": "{\\"step_info\\": \\"foo\\"",
                    "action": "action",
                    "step": "ERROR",
                    "step_time": "30"
                  },
                  "error_details": "{\\"type\\":\\"illegal_argument_exception\\",\\"reason\\":\\"failure\\",\
                \\"stack_trace\\":\\"java.lang.IllegalArgumentException: failure""".replaceAll("\\s", "")));
        }
    }

    public void testTruncateLongError() throws IOException {
        String longError = randomAlphaOfLength(LifecycleExecutionState.MAXIMUM_STEP_INFO_STRING_LENGTH + 20);

        ILMHistoryItem failure = ILMHistoryItem.failure(
            "index",
            "policy",
            1234L,
            100L,
            LifecycleExecutionState.EMPTY_STATE,
            new IllegalArgumentException(longError)
        );

        try (XContentBuilder builder = jsonBuilder()) {
            failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = Strings.toString(builder);
            try (XContentParser p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, json)) {
                Map<String, Object> item = p.map();
                assertThat(
                    (String) item.get("error_details"),
                    containsString(
                        "{\"type\":\"illegal_argument_exception\",\"reason\":\""
                            // We subtract a number of characters here due to the truncation being based
                            // on the length of the whole string, not just the "reason" part.
                            + longError.substring(0, LifecycleExecutionState.MAXIMUM_STEP_INFO_STRING_LENGTH - 47)
                    )
                );
                assertThat((String) item.get("error_details"), matchesPattern(".*\\.\\.\\. \\(\\d+ chars truncated\\).*"));
            }
        }
    }
}
