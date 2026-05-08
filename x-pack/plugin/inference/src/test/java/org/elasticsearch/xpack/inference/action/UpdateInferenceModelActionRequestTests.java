/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class UpdateInferenceModelActionRequestTests extends AbstractWireSerializingTestCase<UpdateInferenceModelAction.Request> {

    private static final String TEST_URL = "https://example.com";
    private static final int TEST_MAX_INPUT_TOKENS = 256;
    private static final int TEST_REQUESTS_PER_MINUTE = 100;
    private static final int TEST_INVALID_TASK_TYPE_VALUE = 42;
    private static final String TEST_NON_MAP_SETTINGS_VALUE = "not_a_map";
    private static final String TEST_UNRECOGNIZED_FIELD = "unrecognized_top_level_field";
    private static final String TEST_UNRECOGNIZED_VALUE = "value";

    @Override
    protected Writeable.Reader<UpdateInferenceModelAction.Request> instanceReader() {
        return UpdateInferenceModelAction.Request::new;
    }

    @Override
    protected UpdateInferenceModelAction.Request createTestInstance() {
        return new UpdateInferenceModelAction.Request(
            randomAlphaOfLength(5),
            randomBytesReference(50),
            randomFrom(XContentType.values()),
            randomFrom(TaskType.values()),
            randomTimeValue()
        );
    }

    @Override
    protected UpdateInferenceModelAction.Request mutateInstance(UpdateInferenceModelAction.Request instance) throws IOException {
        var inferenceId = instance.getInferenceEntityId();
        var content = instance.getContent();
        var contentType = instance.getContentType();
        var taskType = instance.getTaskType();
        switch (randomInt(3)) {
            case 0 -> inferenceId = randomValueOtherThan(inferenceId, () -> randomAlphaOfLength(5));
            case 1 -> content = randomValueOtherThan(content, () -> randomBytesReference(50));
            case 2 -> contentType = randomValueOtherThan(contentType, () -> randomFrom(XContentType.values()));
            case 3 -> taskType = randomValueOtherThan(taskType, () -> randomFrom(TaskType.values()));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new UpdateInferenceModelAction.Request(inferenceId, content, contentType, taskType, randomTimeValue());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }

    public void testParseContent_ReturnsFreshMapInstancesOnEachCall() {
        var request = requestWithBody(Strings.format("""
            {
                "service_settings": {
                    "url": "%s",
                    "rate_limit": {
                        "requests_per_minute": %d
                    }
                },
                "task_settings": {
                    "max_input_tokens": %d
                }
            }""", TEST_URL, TEST_REQUESTS_PER_MINUTE, TEST_MAX_INPUT_TOKENS));

        var firstServiceSettings = request.getServiceSettings();
        var secondServiceSettings = request.getServiceSettings();
        var firstTaskSettings = request.getTaskSettings();
        var secondTaskSettings = request.getTaskSettings();

        assertThat(firstServiceSettings, not(sameInstance(secondServiceSettings)));
        assertThat(firstTaskSettings, not(sameInstance(secondTaskSettings)));

        assertThat(firstServiceSettings, equalTo(secondServiceSettings));
        assertThat(firstTaskSettings, equalTo(secondTaskSettings));
        assertThat(request.getBodyTaskType(), is(nullValue()));
    }

    public void testParseContent_DeepCopiesNestedMaps() {
        var request = requestWithBody(Strings.format("""
            {
                "service_settings": {
                    "rate_limit": {
                        "requests_per_minute": %d
                    }
                }
            }""", TEST_REQUESTS_PER_MINUTE));

        @SuppressWarnings("unchecked")
        var firstRateLimit = (Map<String, Object>) request.getServiceSettings().get(RateLimitSettings.FIELD_NAME);
        @SuppressWarnings("unchecked")
        var secondRateLimit = (Map<String, Object>) request.getServiceSettings().get(RateLimitSettings.FIELD_NAME);

        assertThat(firstRateLimit, not(sameInstance(secondRateLimit)));
        assertThat(firstRateLimit, equalTo(secondRateLimit));
    }

    public void testParseContent_MutatingReturnedMapsDoesNotCorruptCache() {
        var request = requestWithBody(Strings.format("""
            {
                "service_settings": {
                    "url": "%s",
                    "rate_limit": {
                        "requests_per_minute": %d
                    }
                },
                "task_settings": {
                    "max_input_tokens": %d
                }
            }""", TEST_URL, TEST_REQUESTS_PER_MINUTE, TEST_MAX_INPUT_TOKENS));

        var firstServiceSettings = request.getServiceSettings();
        firstServiceSettings.remove(ServiceFields.URL);
        @SuppressWarnings("unchecked")
        var firstRateLimit = (Map<String, Object>) firstServiceSettings.get(RateLimitSettings.FIELD_NAME);
        firstRateLimit.remove(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD);
        request.getTaskSettings().remove(ServiceFields.MAX_INPUT_TOKENS);

        var secondServiceSettings = request.getServiceSettings();
        assertThat(secondServiceSettings.get(ServiceFields.URL), is(TEST_URL));
        @SuppressWarnings("unchecked")
        var secondRateLimit = (Map<String, Object>) secondServiceSettings.get(RateLimitSettings.FIELD_NAME);
        assertThat(secondRateLimit.get(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD), is(TEST_REQUESTS_PER_MINUTE));
        assertThat(request.getTaskSettings().get(ServiceFields.MAX_INPUT_TOKENS), is(TEST_MAX_INPUT_TOKENS));
    }

    public void testParseContent_OmittedSectionsRemainNullAcrossCalls() {
        var request = requestWithBody("""
            {
                "task_type": "text_embedding"
            }""");

        assertThat(request.getServiceSettings(), is(nullValue()));
        assertThat(request.getTaskSettings(), is(nullValue()));
        assertThat(request.getBodyTaskType(), is(TaskType.TEXT_EMBEDDING));
        assertThat(request.getServiceSettings(), is(nullValue()));
        assertThat(request.getTaskSettings(), is(nullValue()));
        assertThat(request.getBodyTaskType(), is(TaskType.TEXT_EMBEDDING));
    }

    public void testParseContent_EmptyBody_ThrowsBadRequest() {
        var request = requestWithBody("{}");

        var exception = expectThrows(ElasticsearchStatusException.class, request::getServiceSettings);
        assertThat(exception.getMessage(), is("Request body is empty"));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
    }

    public void testParseContent_TaskTypeIsNotString_ThrowsBadRequest() {
        var request = requestWithBody(Strings.format("""
            {
                "task_type": %d
            }""", TEST_INVALID_TASK_TYPE_VALUE));

        var exception = expectThrows(ElasticsearchStatusException.class, request::getBodyTaskType);
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(Strings.format("Failed to parse [task_type] in update request [{task_type=%d}]", TEST_INVALID_TASK_TYPE_VALUE))
        );
    }

    public void testParseContent_ServiceSettingsAreNotMap_ThrowsBadRequest() {
        var request = requestWithBody(Strings.format("""
            {
                "service_settings": "%s"
            }""", TEST_NON_MAP_SETTINGS_VALUE));

        var exception = expectThrows(ElasticsearchStatusException.class, request::getServiceSettings);
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(Strings.format("Unable to parse [service_settings] in the request [{service_settings=%s}]", TEST_NON_MAP_SETTINGS_VALUE))
        );
    }

    public void testParseContent_TaskSettingsAreNotMap_ThrowsBadRequest() {
        var request = requestWithBody(Strings.format("""
            {
                "task_settings": "%s"
            }""", TEST_NON_MAP_SETTINGS_VALUE));

        var exception = expectThrows(ElasticsearchStatusException.class, request::getTaskSettings);
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(Strings.format("Unable to parse [task_settings] in the request [{task_settings=%s}]", TEST_NON_MAP_SETTINGS_VALUE))
        );
    }

    public void testParseContent_UnknownTopLevelField_ThrowsBadRequest() {
        var request = requestWithBody(Strings.format("""
            {
                "task_type": "text_embedding",
                "%s": "%s"
            }""", TEST_UNRECOGNIZED_FIELD, TEST_UNRECOGNIZED_VALUE));

        var exception = expectThrows(ElasticsearchStatusException.class, request::getServiceSettings);
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Request contained fields which cannot be updated, remove these fields and try again [{%s=%s}]",
                    TEST_UNRECOGNIZED_FIELD,
                    TEST_UNRECOGNIZED_VALUE
                )
            )
        );
    }

    private static UpdateInferenceModelAction.Request requestWithBody(String body) {
        return new UpdateInferenceModelAction.Request(
            "test_inference_id",
            new BytesArray(body),
            XContentType.JSON,
            TaskType.TEXT_EMBEDDING,
            TimeValue.timeValueSeconds(1)
        );
    }
}
