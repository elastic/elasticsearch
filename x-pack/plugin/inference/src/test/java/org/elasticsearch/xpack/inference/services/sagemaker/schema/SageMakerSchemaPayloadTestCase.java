/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import software.amazon.awssdk.core.SdkBytes;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.junit.Before;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase.toMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class SageMakerSchemaPayloadTestCase<T extends SageMakerSchemaPayload> extends ESTestCase {
    protected T payload;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        payload = payload();
    }

    protected abstract T payload();

    protected abstract String expectedApi();

    protected abstract Set<TaskType> expectedSupportedTaskTypes();

    protected abstract SageMakerStoredServiceSchema randomApiServiceSettings();

    protected abstract SageMakerStoredTaskSchema randomApiTaskSettings();

    public final void testApi() {
        assertThat(payload.api(), equalTo(expectedApi()));
    }

    public final void testSupportedTaskTypes() {
        assertThat(payload.supportedTasks(), containsInAnyOrder(expectedSupportedTaskTypes().toArray()));
    }

    public final void testApiServiceSettings() throws IOException {
        var validationException = new ValidationException();
        var expectedApiServiceSettings = randomApiServiceSettings();
        var actualApiServiceSettings = payload.apiServiceSettings(toMap(expectedApiServiceSettings), validationException);
        assertThat(actualApiServiceSettings, equalTo(expectedApiServiceSettings));
        validationException.throwIfValidationErrorsExist();
    }

    public final void testApiTaskSettings() throws IOException {
        var validationException = new ValidationException();
        var expectedApiTaskSettings = randomApiTaskSettings();
        var actualApiTaskSettings = payload.apiTaskSettings(toMap(expectedApiTaskSettings), validationException);
        assertThat(actualApiTaskSettings, equalTo(expectedApiTaskSettings));
        validationException.throwIfValidationErrorsExist();
    }

    public void testNamedWriteables() {
        var namedWriteables = payload.namedWriteables().map(entry -> entry.name).toList();

        var filteredNames = Set.of(
            SageMakerStoredServiceSchema.NO_OP.getWriteableName(),
            SageMakerStoredTaskSchema.NO_OP.getWriteableName()
        );
        var expectedWriteables = Stream.of(randomApiServiceSettings(), randomApiTaskSettings())
            .map(VersionedNamedWriteable::getWriteableName)
            .filter(Predicate.not(filteredNames::contains))
            .toArray();
        assertThat(namedWriteables, containsInAnyOrder(expectedWriteables));
    }

    public final void testWithUnknownApiServiceSettings() {
        SageMakerModel model = mock();
        when(model.apiServiceSettings()).thenReturn(mock());
        when(model.apiTaskSettings()).thenReturn(randomApiTaskSettings());
        when(model.api()).thenReturn("serviceApi");
        when(model.getTaskType()).thenReturn(TaskType.ANY);

        var e = assertThrows(IllegalArgumentException.class, () -> payload.requestBytes(model, randomRequest()));

        assertThat(e.getMessage(), startsWith("Unsupported SageMaker settings for api [serviceApi] and task type [any]:"));
    }

    public final void testWithUnknownApiTaskSettings() {
        SageMakerModel model = mock();
        when(model.apiServiceSettings()).thenReturn(randomApiServiceSettings());
        when(model.apiTaskSettings()).thenReturn(mock());
        when(model.api()).thenReturn("taskApi");
        when(model.getTaskType()).thenReturn(TaskType.ANY);

        var e = assertThrows(IllegalArgumentException.class, () -> payload.requestBytes(model, randomRequest()));

        assertThat(e.getMessage(), startsWith("Unsupported SageMaker settings for api [taskApi] and task type [any]:"));
    }

    public final void testUpdate() throws IOException {
        var taskSettings = randomApiTaskSettings();
        if (taskSettings != SageMakerStoredTaskSchema.NO_OP) {
            var otherTaskSettings = randomValueOtherThan(taskSettings, this::randomApiTaskSettings);

            var updatedSettings = toMap(taskSettings.updatedTaskSettings(toMap(otherTaskSettings)));

            var initialSettings = toMap(taskSettings);
            var newSettings = toMap(otherTaskSettings);

            newSettings.forEach((key, value) -> {
                assertThat("Value should have been updated for key " + key, value, equalTo(updatedSettings.remove(key)));
            });
            initialSettings.forEach((key, value) -> {
                if (updatedSettings.containsKey(key)) {
                    assertThat("Value should not have been updated for key " + key, value, equalTo(updatedSettings.remove(key)));
                }
            });
            assertTrue("Map should be empty now that we verified all updated keys and all initial keys", updatedSettings.isEmpty());
        }
        if (payload instanceof SageMakerStoredTaskSchema taskSchema) {
            var otherTaskSettings = randomValueOtherThan(randomApiTaskSettings(), this::randomApiTaskSettings);
            var otherTaskSettingsAsMap = toMap(otherTaskSettings);

            taskSchema.updatedTaskSettings(otherTaskSettingsAsMap);
        }
    }

    protected static SageMakerInferenceRequest randomRequest() {
        return new SageMakerInferenceRequest(
            randomBoolean() ? randomAlphaOfLengthBetween(4, 8) : null,
            randomOptionalBoolean(),
            randomBoolean() ? randomInt() : null,
            randomList(randomIntBetween(2, 4), () -> randomAlphaOfLengthBetween(2, 4)),
            randomBoolean(),
            randomFrom(InputType.values())
        );
    }

    protected static void assertSdkBytes(SdkBytes sdkBytes, String expectedValue) {
        assertThat(sdkBytes.asUtf8String(), equalTo(expectedValue));
    }
}
