/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
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

public class CustomTaskSettingsTests extends AbstractWireSerializingTestCase<CustomTaskSettings> {
    public static CustomTaskSettings createRandom() {
        var parameters = randomBoolean() ? randomMap(0, 5, () -> tuple(randomAlphaOfLength(5), (Object) randomAlphaOfLength(5))) : null;
        var ignorePlaceholderCheck = randomBoolean();
        return new CustomTaskSettings(parameters, ignorePlaceholderCheck);
    }

    public void testFromMap() {
        Map<String, Object> taskSettingsMap = new HashMap<>(
            Map.of(
                CustomTaskSettings.PARAMETERS,
                new HashMap<>(Map.of("test_key", "test_value")),
                CustomTaskSettings.IGNORE_PLACEHOLDER_CHECK,
                true
            )
        );

        MatcherAssert.assertThat(
            CustomTaskSettings.fromMap(taskSettingsMap),
            is(new CustomTaskSettings(Map.of("test_key", "test_value"), true))
        );
    }

    public void testXContent() throws IOException {
        var entity = new CustomTaskSettings(Map.of("test_key", "test_value"), true);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{\"parameters\":{\"test_key\":\"test_value\"},\"ignore_placeholder_check\":true}"));
    }

    @Override
    protected Writeable.Reader<CustomTaskSettings> instanceReader() {
        return CustomTaskSettings::new;
    }

    @Override
    protected CustomTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CustomTaskSettings mutateInstance(CustomTaskSettings instance) throws IOException {
        return null;
    }

    public static Map<String, Object> getTaskSettingsMap(
        @Nullable Map<String, Object> parameters,
        @Nullable Boolean ignorePlaceholderCheck
    ) {
        var map = new HashMap<String, Object>();
        if (parameters != null) {
            map.put(CustomTaskSettings.PARAMETERS, parameters);
        }
        if (ignorePlaceholderCheck != null) {
            map.put(CustomTaskSettings.IGNORE_PLACEHOLDER_CHECK, ignorePlaceholderCheck);
        }

        return map;
    }
}
