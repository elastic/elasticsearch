/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests;

import java.util.HashMap;

import static org.elasticsearch.xpack.inference.services.deepseek.DeepSeekChatCompletionServiceSettingsTests.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.deepseek.DeepSeekChatCompletionServiceSettingsTests.TEST_URL;
import static org.elasticsearch.xpack.inference.services.deepseek.DeepSeekChatCompletionServiceSettingsTests.buildServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings.API_KEY;
import static org.hamcrest.Matchers.is;

public class DeepSeekChatCompletionModelTests extends ESTestCase {

    private static final String TEST_API_KEY = "test_api_key";
    public static final String TEST_INFERENCE_ID = "inference_id";
    private static final String INVALID_TEST_URL = "^^^";

    public void testCreateFromNewInput_AllFields_CreatesModelCorrectly() {
        HashMap<String, Object> serviceSettingsMap = buildServiceSettingsMap(TEST_URL, TEST_MODEL_ID);
        serviceSettingsMap.put(API_KEY, TEST_API_KEY);
        var model = DeepSeekChatCompletionModel.createFromNewInput(
            TEST_INFERENCE_ID,
            TaskType.CHAT_COMPLETION,
            DeepSeekService.NAME,
            serviceSettingsMap
        );
        assertThat(
            model,
            is(
                new DeepSeekChatCompletionModel(
                    new ModelConfigurations(
                        TEST_INFERENCE_ID,
                        TaskType.CHAT_COMPLETION,
                        DeepSeekService.NAME,
                        new DeepSeekChatCompletionModel.DeepSeekServiceSettings(TEST_MODEL_ID, ServiceUtils.createUri(TEST_URL))
                    ),
                    new ModelSecrets(new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray())))
                )
            )
        );
    }

    public void testCreateFromNewInput_OnlyMandatoryFields_CreatesModelCorrectly() {
        HashMap<String, Object> serviceSettingsMap = buildServiceSettingsMap(null, TEST_MODEL_ID);
        serviceSettingsMap.put(API_KEY, TEST_API_KEY);
        var model = DeepSeekChatCompletionModel.createFromNewInput(
            TEST_INFERENCE_ID,
            TaskType.CHAT_COMPLETION,
            DeepSeekService.NAME,
            serviceSettingsMap
        );
        assertThat(
            model,
            is(
                new DeepSeekChatCompletionModel(
                    new ModelConfigurations(
                        TEST_INFERENCE_ID,
                        TaskType.CHAT_COMPLETION,
                        DeepSeekService.NAME,
                        new DeepSeekChatCompletionModel.DeepSeekServiceSettings(TEST_MODEL_ID, null)
                    ),
                    new ModelSecrets(new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray())))
                )
            )
        );
    }

    public void testCreateFromNewInput_NoModelId_FailsToCreateModel() {
        HashMap<String, Object> serviceSettingsMap = buildServiceSettingsMap(TEST_URL, null);
        serviceSettingsMap.put(API_KEY, TEST_API_KEY);
        var validationException = assertThrows(
            ValidationException.class,
            () -> DeepSeekChatCompletionModel.createFromNewInput(
                TEST_INFERENCE_ID,
                TaskType.CHAT_COMPLETION,
                DeepSeekService.NAME,
                serviceSettingsMap
            )
        );
        assertThat(
            validationException.validationErrors().getFirst(),
            is("[service_settings] does not contain the required setting [model_id]")
        );
        assertThat(validationException.validationErrors().size(), is(1));
    }

    public void testCreateFromNewInput_NoApiKey_FailsToCreateModel() {
        HashMap<String, Object> serviceSettingsMap = buildServiceSettingsMap(TEST_URL, TEST_MODEL_ID);
        var validationException = assertThrows(
            ValidationException.class,
            () -> DeepSeekChatCompletionModel.createFromNewInput(
                TEST_INFERENCE_ID,
                TaskType.CHAT_COMPLETION,
                DeepSeekService.NAME,
                serviceSettingsMap
            )
        );
        assertThat(
            validationException.validationErrors().getFirst(),
            is("[service_settings] does not contain the required setting [api_key]")
        );
        assertThat(validationException.validationErrors().size(), is(1));
    }

    public void testCreateFromNewInput_InvalidUrl_FailsToCreateModel() {
        HashMap<String, Object> serviceSettingsMap = buildServiceSettingsMap(INVALID_TEST_URL, TEST_MODEL_ID);
        serviceSettingsMap.put(API_KEY, TEST_API_KEY);
        var validationException = assertThrows(
            ValidationException.class,
            () -> DeepSeekChatCompletionModel.createFromNewInput(
                TEST_INFERENCE_ID,
                TaskType.CHAT_COMPLETION,
                DeepSeekService.NAME,
                serviceSettingsMap
            )
        );
        assertThat(validationException.validationErrors().getFirst(), is(Strings.format("""
            [service_settings] Invalid url [%s] received for field [url]. Error: unable to parse url [%s]. \
            Reason: Illegal character in path""", INVALID_TEST_URL, INVALID_TEST_URL)));
        assertThat(validationException.validationErrors().size(), is(1));
    }

    public void testReadFromStorage_AllFields_CreatesModelCorrectly() {
        var model = DeepSeekChatCompletionModel.readFromStorage(
            TEST_INFERENCE_ID,
            TaskType.CHAT_COMPLETION,
            DeepSeekService.NAME,
            buildServiceSettingsMap(TEST_URL, TEST_MODEL_ID),
            DefaultSecretSettingsTests.getSecretSettingsMap(TEST_API_KEY)
        );

        assertThat(
            model,
            is(
                new DeepSeekChatCompletionModel(
                    new ModelConfigurations(
                        TEST_INFERENCE_ID,
                        TaskType.CHAT_COMPLETION,
                        DeepSeekService.NAME,
                        new DeepSeekChatCompletionModel.DeepSeekServiceSettings(TEST_MODEL_ID, ServiceUtils.createUri(TEST_URL))
                    ),
                    new ModelSecrets(new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray())))
                )
            )
        );
    }

    public void testReadFromStorage_OnlyMandatoryFields_CreatesModelCorrectly() {
        var model = DeepSeekChatCompletionModel.readFromStorage(
            TEST_INFERENCE_ID,
            TaskType.CHAT_COMPLETION,
            DeepSeekService.NAME,
            buildServiceSettingsMap(null, TEST_MODEL_ID),
            null
        );

        assertThat(
            model,
            is(
                new DeepSeekChatCompletionModel(
                    new ModelConfigurations(
                        TEST_INFERENCE_ID,
                        TaskType.CHAT_COMPLETION,
                        DeepSeekService.NAME,
                        new DeepSeekChatCompletionModel.DeepSeekServiceSettings(TEST_MODEL_ID, null)
                    ),
                    new ModelSecrets()
                )
            )
        );
    }

    public void testReadFromStorage_NoModelId_FailsToCreateModel() {
        var validationException = assertThrows(
            ValidationException.class,
            () -> DeepSeekChatCompletionModel.readFromStorage(
                TEST_INFERENCE_ID,
                TaskType.CHAT_COMPLETION,
                DeepSeekService.NAME,
                buildServiceSettingsMap(TEST_URL, null),
                DefaultSecretSettingsTests.getSecretSettingsMap(TEST_API_KEY)
            )
        );

        assertThat(
            validationException.validationErrors().getFirst(),
            is("[service_settings] does not contain the required setting [model_id]")
        );
        assertThat(validationException.validationErrors().size(), is(1));
    }

    public void testReadFromStorage_InvalidUrl_FailsToCreateModel() {
        var validationException = assertThrows(
            ValidationException.class,
            () -> DeepSeekChatCompletionModel.readFromStorage(
                TEST_INFERENCE_ID,
                TaskType.CHAT_COMPLETION,
                DeepSeekService.NAME,
                buildServiceSettingsMap(INVALID_TEST_URL, TEST_MODEL_ID),
                DefaultSecretSettingsTests.getSecretSettingsMap(TEST_API_KEY)
            )
        );

        assertThat(validationException.validationErrors().getFirst(), is(Strings.format("""
            [service_settings] Invalid url [%s] received for field [url]. Error: unable to parse url [%s]. \
            Reason: Illegal character in path""", INVALID_TEST_URL, INVALID_TEST_URL)));
        assertThat(validationException.validationErrors().size(), is(1));
    }
}
