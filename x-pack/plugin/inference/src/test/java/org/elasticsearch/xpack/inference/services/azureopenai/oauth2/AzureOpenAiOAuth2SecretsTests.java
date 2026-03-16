/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.oauth2;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettingsTests.CLIENT_SECRET_VALUE;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettingsTests.TEST_INFERENCE_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiOAuth2SecretsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiOAuth2Secrets> {

    public static AzureOpenAiOAuth2Secrets createRandom() {
        return new AzureOpenAiOAuth2Secrets(randomAlphaOfLength(10), randomSecureStringOfLength(15));
    }

    public void testNewSecretSettings_ClientSecret() {
        var initialSettings = createRandom();
        var clientSecret = randomSecureStringOfLength(15);
        var newSettings = new AzureOpenAiOAuth2Secrets(initialSettings.getInferenceId(), clientSecret);
        var finalSettings = (AzureOpenAiOAuth2Secrets) initialSettings.newSecretSettings(
            Map.of(CLIENT_SECRET_FIELD, clientSecret.toString())
        );

        assertThat(finalSettings, is(newSettings));
    }

    public void testToXContent_WritesClientSecretWhenSet() throws IOException {
        var testSettings = new AzureOpenAiOAuth2Secrets(TEST_INFERENCE_ID, new SecureString(CLIENT_SECRET_VALUE.toCharArray()));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        var expectedResult = XContentHelper.stripWhitespace(Strings.format("""
            {
                "%s":"%s"
            }""", CLIENT_SECRET_FIELD, CLIENT_SECRET_VALUE));
        assertThat(xContentResult, is(expectedResult));
    }

    public void testFromMap_Empty_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, "")), TEST_INFERENCE_ID)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("[secret_settings] Invalid value empty string. [%s] must be a non-empty string", CLIENT_SECRET_FIELD)
            )
        );
    }

    @Override
    protected Writeable.Reader<AzureOpenAiOAuth2Secrets> instanceReader() {
        return AzureOpenAiOAuth2Secrets::new;
    }

    @Override
    protected AzureOpenAiOAuth2Secrets createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureOpenAiOAuth2Secrets mutateInstance(AzureOpenAiOAuth2Secrets instance) throws IOException {
        var inferenceId = instance.getInferenceId();
        var clientSecret = instance.getClientSecret();

        switch (randomInt(1)) {
            case 0 -> inferenceId = randomValueOtherThan(inferenceId, () -> randomAlphaOfLength(10));
            case 1 -> clientSecret = randomValueOtherThan(clientSecret, () -> randomSecureStringOfLength(15));
            default -> throw new AssertionError("Illegal randomization branch");
        }

        return new AzureOpenAiOAuth2Secrets(inferenceId, clientSecret);
    }

    @Override
    protected AzureOpenAiOAuth2Secrets mutateInstanceForVersion(AzureOpenAiOAuth2Secrets instance, TransportVersion version) {
        return instance;
    }
}
