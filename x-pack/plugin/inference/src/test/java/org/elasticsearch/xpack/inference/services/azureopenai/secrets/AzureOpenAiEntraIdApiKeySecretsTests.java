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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.ENTRA_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettingsTests.TEST_API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettingsTests.TEST_ENTRA_ID;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiEntraIdApiKeySecretsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiEntraIdApiKeySecrets> {

    public static AzureOpenAiEntraIdApiKeySecrets createRandomEntraIdApiKeySecrets() {
        if (randomBoolean()) {
            return new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(15), null);
        }
        return new AzureOpenAiEntraIdApiKeySecrets(null, randomSecureStringOfLength(15));
    }

    /**
     * Creates a {@link AzureOpenAiEntraIdApiKeySecrets} instance with either the API key or Entra ID set,
     * based on which parameter is non-null. API key takes precedence if both are provided,
     * but typically only one should be provided as they are mutually exclusive.
     */
    public static AzureOpenAiEntraIdApiKeySecrets createSecret(@Nullable String apiKey, @Nullable String entraId) {
        if (apiKey != null) {
            return new AzureOpenAiEntraIdApiKeySecrets(new SecureString(apiKey.toCharArray()), null);
        } else if (entraId != null) {
            return new AzureOpenAiEntraIdApiKeySecrets(null, new SecureString(entraId.toCharArray()));
        } else {
            throw new IllegalArgumentException("Either apiKey or entraId must be provided");
        }
    }

    public void testOnlyOneSecretFieldSet() {
        expectThrows(NullPointerException.class, () -> new AzureOpenAiEntraIdApiKeySecrets(null, null));

        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new AzureOpenAiEntraIdApiKeySecrets(new SecureString("apiKey".toCharArray()), new SecureString("entraId".toCharArray()))
        );
        assertThat(exception.getMessage(), is("Only one of apiKey or entraId can be set, but both were provided"));
    }

    public void testNewSecretSettingsApiKey() {
        var initialSettings = createRandomEntraIdApiKeySecrets();
        var apiKey = randomSecureStringOfLength(15);
        var expectedSettings = new AzureOpenAiEntraIdApiKeySecrets(apiKey, null);
        var newSettings = (AzureOpenAiEntraIdApiKeySecrets) initialSettings.newSecretSettings(Map.of(API_KEY, apiKey.toString()));

        assertThat(newSettings, is(expectedSettings));
    }

    public void testNewSecretSettingsEntraId() {
        var initialSettings = createRandomEntraIdApiKeySecrets();
        var entraId = randomSecureStringOfLength(15);
        var expectedSettings = new AzureOpenAiEntraIdApiKeySecrets(null, entraId);
        var newSettings = (AzureOpenAiEntraIdApiKeySecrets) initialSettings.newSecretSettings(Map.of(ENTRA_ID, entraId.toString()));

        assertThat(newSettings, is(expectedSettings));
    }

    public void testToXContext_WritesApiKeyOnlyWhenApiKeySet() throws IOException {
        var testSettings = new AzureOpenAiEntraIdApiKeySecrets(new SecureString(TEST_API_KEY.toCharArray()), null);

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
        var testSettings = new AzureOpenAiEntraIdApiKeySecrets(null, new SecureString(TEST_ENTRA_ID.toCharArray()));
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
        var apiKey = instance.apiKey();
        var entraId = instance.entraId();

        switch (randomInt(1)) {
            case 0 -> {
                if (apiKey == null) {
                    return new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(15), null);
                }

                if (randomBoolean()) {
                    return new AzureOpenAiEntraIdApiKeySecrets(
                        randomValueOtherThan(instance.apiKey(), () -> randomSecureStringOfLength(15)),
                        null
                    );
                } else {
                    return new AzureOpenAiEntraIdApiKeySecrets(null, randomSecureStringOfLength(15));
                }
            }
            case 1 -> {
                if (entraId == null) {
                    return new AzureOpenAiEntraIdApiKeySecrets(null, randomSecureStringOfLength(15));
                }

                if (randomBoolean()) {
                    return new AzureOpenAiEntraIdApiKeySecrets(
                        null,
                        randomValueOtherThan(instance.entraId(), () -> randomSecureStringOfLength(15))
                    );
                } else {
                    return new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(15), null);
                }
            }
            default -> throw new AssertionError("Illegal randomization branch");
        }
    }

    @Override
    protected AzureOpenAiEntraIdApiKeySecrets mutateInstanceForVersion(AzureOpenAiEntraIdApiKeySecrets instance, TransportVersion version) {
        return instance;
    }
}
