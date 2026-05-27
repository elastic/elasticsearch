/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.secrets;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class OpenAiOAuth2SecretsSettingsTests extends AbstractBWCWireSerializationTestCase<OpenAiOAuth2SecretsSettings> {

    public static OpenAiOAuth2SecretsSettings createRandom() {
        return new OpenAiOAuth2SecretsSettings(new SecureString(randomAlphaOfLength(20).toCharArray()));
    }

    @Override
    protected Writeable.Reader<OpenAiOAuth2SecretsSettings> instanceReader() {
        return OpenAiOAuth2SecretsSettings::new;
    }

    @Override
    protected OpenAiOAuth2SecretsSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenAiOAuth2SecretsSettings mutateInstance(OpenAiOAuth2SecretsSettings instance) throws IOException {
        return randomValueOtherThan(instance, OpenAiOAuth2SecretsSettingsTests::createRandom);
    }

    @Override
    protected OpenAiOAuth2SecretsSettings mutateInstanceForVersion(OpenAiOAuth2SecretsSettings instance, TransportVersion version) {
        return instance;
    }

    public void testClientSecretAccessor() {
        var secret = new SecureString("super-secret".toCharArray());
        var secrets = new OpenAiOAuth2SecretsSettings(secret);
        assertEquals(secret, secrets.clientSecret());
    }
}
