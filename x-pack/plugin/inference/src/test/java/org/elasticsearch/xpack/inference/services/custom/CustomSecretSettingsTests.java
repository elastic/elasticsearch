/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.is;

public class CustomSecretSettingsTests extends AbstractWireSerializingTestCase<CustomSecretSettings> {
    public static CustomSecretSettings createRandom() {
        var secretParameters = randomBoolean()
            ? randomMap(0, 5, () -> tuple(randomAlphaOfLength(5), (Object) randomAlphaOfLength(5)))
            : null;
        return new CustomSecretSettings(secretParameters);
    }

    public void testFromMap() {
        Map<String, Object> secretParameters = new HashMap<>(
            Map.of(CustomSecretSettings.SECRET_PARAMETERS, new HashMap<>(Map.of("test_key", "test_value")))
        );

        MatcherAssert.assertThat(
            CustomSecretSettings.fromMap(secretParameters),
            is(new CustomSecretSettings(Map.of("test_key", "test_value")))
        );
    }

    public void testXContent() throws IOException {
        var entity = new CustomSecretSettings(Map.of("test_key", "test_value"));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{\"secret_parameters\":{\"test_key\":\"test_value\"}}"));
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
        return null;
    }
}
