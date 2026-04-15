/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2SecretsTests.CLIENT_SECRET_VALUE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiOAuth2SecretsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiOAuth2Secrets> {

    public static AzureOpenAiOAuth2Secrets createRandom() {
        return new AzureOpenAiOAuth2Secrets(randomSecureStringOfLength(15));
    }

    public void testNewSecretSettings_ClientSecret() {
        var initialSettings = createRandom();
        var clientSecret = randomSecureStringOfLength(15);
        var expectedSettings = new AzureOpenAiOAuth2Secrets(clientSecret);
        var newSettings = (AzureOpenAiOAuth2Secrets) initialSettings.newSecretSettings(
            new HashMap<>(Map.of(CLIENT_SECRET_FIELD, clientSecret.toString()))
        );

        assertThat(newSettings, is(expectedSettings));
    }

    public void testToXContent_WritesClientSecretWhenSet() throws IOException {
        var testSettings = new AzureOpenAiOAuth2Secrets(new SecureString(CLIENT_SECRET_VALUE.toCharArray()));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        var expectedResult = XContentHelper.stripWhitespace(Strings.format("""
            {
                "%s":"%s"
            }""", CLIENT_SECRET_FIELD, CLIENT_SECRET_VALUE));
        assertThat(xContentResult, is(expectedResult));
    }

    public void testFromMap_EmptyClientSecretValue_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretSettings.fromMap(new HashMap<>(Map.of(CLIENT_SECRET_FIELD, "")))
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
        return new AzureOpenAiOAuth2Secrets(randomValueOtherThan(instance.getClientSecret(), () -> randomSecureStringOfLength(15)));
    }

    @Override
    protected AzureOpenAiOAuth2Secrets mutateInstanceForVersion(AzureOpenAiOAuth2Secrets instance, TransportVersion version) {
        return instance;
    }
}
