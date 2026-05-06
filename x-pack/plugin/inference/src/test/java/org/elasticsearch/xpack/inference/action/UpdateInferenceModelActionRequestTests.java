/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
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

    public void testGetContentAsSettings_ReturnsFreshSettingsInstanceOnEachCall() {
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

        var first = request.getContentAsSettings();
        var second = request.getContentAsSettings();

        assertThat(first, not(sameInstance(second)));
        assertThat(first.serviceSettings(), not(sameInstance(second.serviceSettings())));
        assertThat(first.taskSettings(), not(sameInstance(second.taskSettings())));

        assertThat(first.serviceSettings(), equalTo(second.serviceSettings()));
        assertThat(first.taskSettings(), equalTo(second.taskSettings()));
        assertThat(first.taskType(), sameInstance(second.taskType()));
    }

    public void testGetContentAsSettings_DeepCopiesNestedMaps() {
        var request = requestWithBody(Strings.format("""
            {
                "service_settings": {
                    "rate_limit": {
                        "requests_per_minute": %d
                    }
                }
            }""", TEST_REQUESTS_PER_MINUTE));

        @SuppressWarnings("unchecked")
        var firstRateLimit = (Map<String, Object>) request.getContentAsSettings().serviceSettings().get(RateLimitSettings.FIELD_NAME);
        @SuppressWarnings("unchecked")
        var secondRateLimit = (Map<String, Object>) request.getContentAsSettings().serviceSettings().get(RateLimitSettings.FIELD_NAME);

        assertThat(firstRateLimit, not(sameInstance(secondRateLimit)));
        assertThat(firstRateLimit, equalTo(secondRateLimit));
    }

    public void testGetContentAsSettings_MutatingReturnedMapsDoesNotCorruptCache() {
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

        var first = request.getContentAsSettings();
        first.serviceSettings().remove(ServiceFields.URL);
        @SuppressWarnings("unchecked")
        var firstRateLimit = (Map<String, Object>) first.serviceSettings().get(RateLimitSettings.FIELD_NAME);
        firstRateLimit.remove(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD);
        first.taskSettings().remove(ServiceFields.MAX_INPUT_TOKENS);

        var second = request.getContentAsSettings();
        assertThat(second.serviceSettings().get(ServiceFields.URL), is(TEST_URL));
        @SuppressWarnings("unchecked")
        var secondRateLimit = (Map<String, Object>) second.serviceSettings().get(RateLimitSettings.FIELD_NAME);
        assertThat(secondRateLimit.get(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD), is(TEST_REQUESTS_PER_MINUTE));
        assertThat(second.taskSettings().get(ServiceFields.MAX_INPUT_TOKENS), is(TEST_MAX_INPUT_TOKENS));
    }

    public void testGetContentAsSettings_ReturnedMapsAreModifiableAtAllDepths() {
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

        var settings = request.getContentAsSettings();
        assertThat(settings.serviceSettings().remove(ServiceFields.URL), is(TEST_URL));
        assertThat(settings.taskSettings().remove(ServiceFields.MAX_INPUT_TOKENS), is(TEST_MAX_INPUT_TOKENS));
        @SuppressWarnings("unchecked")
        var rateLimit = (Map<String, Object>) settings.serviceSettings().get(RateLimitSettings.FIELD_NAME);
        assertThat(rateLimit.remove(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD), is(TEST_REQUESTS_PER_MINUTE));
    }

    public void testGetContentAsSettings_OmittedSectionsRemainNullAcrossCalls() {
        var request = requestWithBody("""
            {
                "task_type": "text_embedding"
            }""");

        var first = request.getContentAsSettings();
        var second = request.getContentAsSettings();

        assertThat(first.serviceSettings(), is(nullValue()));
        assertThat(first.taskSettings(), is(nullValue()));
        assertThat(first.taskType(), is(TaskType.TEXT_EMBEDDING));
        assertThat(second.serviceSettings(), is(nullValue()));
        assertThat(second.taskSettings(), is(nullValue()));
        assertThat(second.taskType(), is(TaskType.TEXT_EMBEDDING));
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
