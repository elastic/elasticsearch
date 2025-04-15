/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

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
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.is;

public class CustomSecretSettingsTests extends AbstractBWCWireSerializationTestCase<CustomSecretSettings> {
    public static CustomSecretSettings createRandom() {
        Map<String, SecureString> secretParameters = randomMap(
            0,
            5,
            () -> tuple(randomAlphaOfLength(5), new SecureString(randomAlphaOfLength(5).toCharArray()))
        );

        return new CustomSecretSettings(secretParameters);
    }

    public void testFromMap() {
        Map<String, Object> secretParameters = new HashMap<>(
            Map.of(CustomSecretSettings.SECRET_PARAMETERS, new HashMap<>(Map.of("test_key", "test_value")))
        );

        assertThat(
            CustomSecretSettings.fromMap(secretParameters),
            is(new CustomSecretSettings(Map.of("test_key", new SecureString("test_value".toCharArray()))))
        );
    }

    public void testXContent() throws IOException {
        var entity = new CustomSecretSettings(Map.of("test_key", new SecureString("test_value".toCharArray())));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "secret_parameters": {
                    "test_key": "test_value"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<CustomSecretSettings> instanceReader() {
        return CustomSecretSettings::new;
    }

    @Override
    protected CustomSecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CustomSecretSettings mutateInstance(CustomSecretSettings instance) {
        return randomValueOtherThan(instance, CustomSecretSettingsTests::createRandom);
    }

    @Override
    protected CustomSecretSettings mutateInstanceForVersion(CustomSecretSettings instance, TransportVersion version) {
        return instance;
    }
}
