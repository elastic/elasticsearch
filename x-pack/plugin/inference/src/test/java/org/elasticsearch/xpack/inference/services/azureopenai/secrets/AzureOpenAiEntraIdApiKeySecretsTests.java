/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.ENTRA_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettingsTests.TEST_API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettingsTests.TEST_ENTRA_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettingsTests.TEST_INFERENCE_ID;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiEntraIdApiKeySecretsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiEntraIdApiKeySecrets> {

    public static AzureOpenAiEntraIdApiKeySecrets createRandomEntraIdApiKeySecrets() {
        if (randomBoolean()) {
            return new AzureOpenAiEntraIdApiKeySecrets(randomAlphaOfLength(10), randomSecureStringOfLength(15), null);
        }
        return new AzureOpenAiEntraIdApiKeySecrets(randomAlphaOfLength(10), null, randomSecureStringOfLength(15));
    }

    public void testNewSecretSettingsApiKey() {
        var initialSettings = createRandomEntraIdApiKeySecrets();
        var apiKey = randomSecureStringOfLength(15);
        var newSettings = new AzureOpenAiEntraIdApiKeySecrets(initialSettings.getInferenceId(), apiKey, null);
        var finalSettings = (AzureOpenAiEntraIdApiKeySecrets) initialSettings.newSecretSettings(Map.of(API_KEY, apiKey.toString()));

        assertThat(finalSettings, is(newSettings));
    }

    public void testNewSecretSettingsEntraId() {
        var initialSettings = createRandomEntraIdApiKeySecrets();
        var entraId = randomSecureStringOfLength(15);
        var newSettings = new AzureOpenAiEntraIdApiKeySecrets(initialSettings.getInferenceId(), null, entraId);
        var finalSettings = (AzureOpenAiEntraIdApiKeySecrets) initialSettings.newSecretSettings(Map.of(ENTRA_ID, entraId.toString()));

        assertEquals(newSettings, finalSettings);
    }

    public void testToXContext_WritesApiKeyOnlyWhenApiKeySet() throws IOException {
        var testSettings = new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, new SecureString(TEST_API_KEY.toCharArray()), null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        var expectedResult = XContentHelper.stripWhitespace(Strings.format("""
            {
                "%s":"%s"
            }""", API_KEY, TEST_API_KEY));
        assertThat(xContentResult, is(expectedResult));
    }

    public void testToXContext_WritesEntraIdOnlyWhenEntraIdSet() throws IOException {
        var testSettings = new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, null, new SecureString(TEST_ENTRA_ID.toCharArray()));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        var expectedResult = XContentHelper.stripWhitespace(Strings.format("""
            {
                "%s":"%s"
            }""", ENTRA_ID, TEST_ENTRA_ID));
        assertThat(xContentResult, is(expectedResult));
    }

    @Override
    protected Writeable.Reader<AzureOpenAiEntraIdApiKeySecrets> instanceReader() {
        return AzureOpenAiEntraIdApiKeySecrets::new;
    }

    @Override
    protected AzureOpenAiEntraIdApiKeySecrets createTestInstance() {
        return createRandomEntraIdApiKeySecrets();
    }

    @Override
    protected AzureOpenAiEntraIdApiKeySecrets mutateInstance(AzureOpenAiEntraIdApiKeySecrets instance) throws IOException {
        var inferenceId = instance.getInferenceId();
        var apiKey = instance.apiKey();
        var entraId = instance.entraId();

        switch (randomInt(2)) {
            case 0 -> {
                return new AzureOpenAiEntraIdApiKeySecrets(randomAlphaOfLength(10), apiKey, entraId);
            }
            case 1 -> {
                if (apiKey == null) {
                    return new AzureOpenAiEntraIdApiKeySecrets(inferenceId, randomSecureStringOfLength(15), null);
                }

                if (randomBoolean()) {
                    return new AzureOpenAiEntraIdApiKeySecrets(
                        inferenceId,
                        randomValueOtherThan(instance.apiKey(), () -> randomSecureStringOfLength(15)),
                        null
                    );
                } else {
                    return new AzureOpenAiEntraIdApiKeySecrets(inferenceId, null, randomSecureStringOfLength(15));
                }
            }
            case 2 -> {
                if (entraId == null) {
                    return new AzureOpenAiEntraIdApiKeySecrets(inferenceId, null, randomSecureStringOfLength(15));
                }

                if (randomBoolean()) {
                    return new AzureOpenAiEntraIdApiKeySecrets(
                        inferenceId,
                        null,
                        randomValueOtherThan(instance.apiKey(), () -> randomSecureStringOfLength(15))
                    );
                } else {
                    return new AzureOpenAiEntraIdApiKeySecrets(inferenceId, randomSecureStringOfLength(15), null);
                }
            }
            default -> throw new AssertionError("Illegal randomization branch");
        }
    }

    @Override
    protected AzureOpenAiEntraIdApiKeySecrets mutateInstanceForVersion(AzureOpenAiEntraIdApiKeySecrets instance, TransportVersion version) {
        if (version.supports(AZURE_OPENAI_OAUTH_SETTINGS)) {
            return instance;
        }

        return new AzureOpenAiEntraIdApiKeySecrets(null, instance.apiKey(), instance.entraId());
    }
}
