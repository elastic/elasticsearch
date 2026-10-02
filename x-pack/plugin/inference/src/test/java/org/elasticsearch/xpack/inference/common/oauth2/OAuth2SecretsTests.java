/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.hamcrest.Matchers.is;

public class OAuth2SecretsTests extends AbstractBWCWireSerializationTestCase<OAuth2Secrets> {

    public static final String TEST_CLIENT_SECRET = "secret";

    public static OAuth2Secrets createRandom() {
        var clientSecret = randomSecureStringOfLength(10);
        return new OAuth2Secrets(clientSecret);
    }

    public void testToXContent_WritesClientSecretWhenSet() throws IOException {
        var testSettings = new OAuth2Secrets(new SecureString(TEST_CLIENT_SECRET.toCharArray()));
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        testSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        var expectedResult = XContentHelper.stripWhitespace(Strings.format("""
            {
                "%s": "%s"
            }
            """, CLIENT_SECRET_FIELD, TEST_CLIENT_SECRET));
        assertThat(xContentResult, is(expectedResult));
    }

    @Override
    protected OAuth2Secrets mutateInstanceForVersion(OAuth2Secrets instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<OAuth2Secrets> instanceReader() {
        return OAuth2Secrets::new;
    }

    @Override
    protected OAuth2Secrets createTestInstance() {
        return createRandom();
    }

    @Override
    protected OAuth2Secrets mutateInstance(OAuth2Secrets instance) throws IOException {
        return new OAuth2Secrets(randomValueOtherThan(instance.clientSecret(), () -> randomSecureStringOfLength(10)));
    }
}
