/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public abstract class OpenAiTaskSettingsTests<T extends OpenAiTaskSettings> extends AbstractBWCWireSerializationTestCase<T> {

    public T createRandom() {
        var user = randomFrom(
            StatefulValue.<String>undefined(),
            StatefulValue.<String>nullInstance(),
            StatefulValue.of(randomAlphaOfLength(15))
        );
        var headers = randomFrom(
            Headers.UNDEFINED_INSTANCE,
            Headers.NULL_INSTANCE,
            new Headers(StatefulValue.of(Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15))))
        );
        return create(user, headers);
    }

    public void testIsEmpty() {
        assertTrue(create(StatefulValue.undefined(), Headers.UNDEFINED_INSTANCE).isEmpty());
        assertTrue(create(StatefulValue.nullInstance(), Headers.UNDEFINED_INSTANCE).isEmpty());
        assertTrue(create(StatefulValue.undefined(), Headers.NULL_INSTANCE).isEmpty());
        assertFalse(create(StatefulValue.of("user"), Headers.UNDEFINED_INSTANCE).isEmpty());
        assertFalse(create(StatefulValue.undefined(), new Headers(StatefulValue.of(Map.of("K", "v")))).isEmpty());
        assertFalse(create(StatefulValue.of("user"), new Headers(StatefulValue.of(Map.of("K", "v")))).isEmpty());
    }

    public void testUpdatedTaskSettings_KeepsOriginalValuesWithEmptyOverrides() {
        var taskSettings = createFromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")), ConfigurationParseContext.REQUEST);

        assertThat(taskSettings.updatedTaskSettings(new HashMap<>()), is(taskSettings));
    }

    public void testUpdatedTaskSettings_OverridesUser() {
        var taskSettings = createFromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")), ConfigurationParseContext.REQUEST);

        var updated = taskSettings.updatedTaskSettings(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user2")));
        assertThat(updated.user(), is(StatefulValue.of("user2")));
        assertTrue(updated.headers().mapValue().isUndefined());
    }

    public void testUpdatedTaskSettings_ClearsUserWithNull() {
        var taskSettings = createFromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")), ConfigurationParseContext.REQUEST);

        var nullUserMap = new HashMap<String, Object>();
        nullUserMap.put(OpenAiServiceFields.USER, null);
        var updated = taskSettings.updatedTaskSettings(nullUserMap);
        assertTrue(updated.user().isUndefined());
    }

    public void testUpdatedTaskSettings_OverridesHeaders() {
        var user = "user";
        var taskSettings = createFromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, user)), ConfigurationParseContext.REQUEST);

        var headers = Map.of("key", "value");
        var updated = taskSettings.updatedTaskSettings(new HashMap<>(Map.of(OpenAiServiceFields.HEADERS, headers)));
        assertThat(updated.user(), is(StatefulValue.of(user)));
        assertThat(updated.headers().mapValue().get(), is(headers));
    }

    public void testFromMap_WithUserAndHeaders() {
        var settings = createFromMap(
            new HashMap<>(Map.of(OpenAiServiceFields.USER, "user", OpenAiServiceFields.HEADERS, Map.of("key", "value"))),
            ConfigurationParseContext.REQUEST
        );
        assertThat(settings.user(), is(StatefulValue.of("user")));
        assertThat(settings.headers().mapValue().get(), is(Map.of("key", "value")));
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> createFromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "")), ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_ProducesUndefined() {
        var taskSettings = createFromMap(new HashMap<>(), ConfigurationParseContext.REQUEST);
        assertTrue(taskSettings.user().isUndefined());
    }

    public void testFromMap_NullUser_ProducesNullInstance() {
        var nullUserMap = new HashMap<String, Object>();
        nullUserMap.put(OpenAiServiceFields.USER, null);
        var settings = createFromMap(nullUserMap, ConfigurationParseContext.REQUEST);
        assertTrue(settings.user().isNull());
    }

    public void testFromMap_PersistentContext_IgnoresUnknownFields() {
        var settings = createFromMap(new HashMap<>(Map.of("unknown_field", "value")), ConfigurationParseContext.PERSISTENT);
        assertTrue(settings.user().isUndefined());
        assertTrue(settings.headers().mapValue().isUndefined());
    }

    public void testFromMap_ParsesCorrectly_WhenHeadersIsEmptyMap() {
        var settings = createFromMap(
            new HashMap<>(Map.of(OpenAiServiceFields.USER, "user", OpenAiServiceFields.HEADERS, Map.of())),
            ConfigurationParseContext.REQUEST
        );
        assertThat(settings.user(), is(StatefulValue.of("user")));
        // empty headers map → NULL_INSTANCE (treated as clearing)
        assertTrue(settings.headers().isNull());
    }

    public void testFromMap_ParsesCorrectly_WhenHeadersMapOfNulls() {
        var headersMap = new HashMap<String, Object>();
        headersMap.put("key1", null);
        headersMap.put("key2", null);
        var settings = createFromMap(
            new HashMap<>(Map.of(OpenAiServiceFields.USER, "user", OpenAiServiceFields.HEADERS, headersMap)),
            ConfigurationParseContext.REQUEST
        );
        assertThat(settings.user(), is(StatefulValue.of("user")));
        // all null values stripped → empty map → NULL_INSTANCE
        assertTrue(settings.headers().isNull());
    }

    public void testFromMap_ParsesCorrectly_WhenHeadersContainsAnInteger() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> createFromMap(
                new HashMap<>(Map.of(OpenAiServiceFields.USER, "user", OpenAiServiceFields.HEADERS, new HashMap<>(Map.of("key", 1)))),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(exception.getMessage(), containsString("failed to parse field [headers]"));
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                "Map field [headers] has an entry that is not valid, [key => 1]. Value type of [Integer] is not one of [String].;"
            )
        );
    }

    @Override
    protected T mutateInstance(T instance) throws IOException {
        return randomBoolean() ? mutateUser(instance) : mutateHeaders(instance);
    }

    private T mutateUser(T instance) {
        var currentUser = instance.user();
        StatefulValue<String> newUser;
        if (currentUser.isUndefined()) {
            newUser = randomBoolean() ? StatefulValue.nullInstance() : StatefulValue.of(randomAlphaOfLength(15));
        } else if (currentUser.isNull()) {
            newUser = randomBoolean() ? StatefulValue.undefined() : StatefulValue.of(randomAlphaOfLength(15));
        } else {
            newUser = randomFrom(StatefulValue.undefined(), StatefulValue.nullInstance(), StatefulValue.of(currentUser.get() + "_mutated"));
        }
        return create(newUser, instance.headers());
    }

    private T mutateHeaders(T instance) {
        var currentHeaders = instance.headers();
        Headers newHeaders;
        if (currentHeaders.mapValue().isUndefined()) {
            newHeaders = randomBoolean()
                ? Headers.NULL_INSTANCE
                : new Headers(StatefulValue.of(Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15))));
        } else if (currentHeaders.isNull()) {
            newHeaders = randomBoolean()
                ? Headers.UNDEFINED_INSTANCE
                : new Headers(StatefulValue.of(Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15))));
        } else {
            var mutatedMap = new HashMap<>(currentHeaders.mapValue().get());
            mutatedMap.put(randomAlphaOfLength(15), randomAlphaOfLength(15));
            newHeaders = randomFrom(Headers.UNDEFINED_INSTANCE, Headers.NULL_INSTANCE, new Headers(StatefulValue.of(mutatedMap)));
        }
        return create(instance.user(), newHeaders);
    }

    public void testToXContent_WritesUserAndHeaders() throws IOException {
        var settings = create(StatefulValue.of("user"), new Headers(StatefulValue.of(Map.of("key", "value"))));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace("""
            {
                "user": "user",
                "headers": {"key": "value"}
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_WritesOnlyUser_WhenHeadersIsUndefined() throws IOException {
        var settings = create(StatefulValue.of("user"), Headers.UNDEFINED_INSTANCE);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace("""
            {
                "user": "user"
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_WritesOnlyHeaders_WhenUserIsUndefined() throws IOException {
        var settings = create(StatefulValue.undefined(), new Headers(StatefulValue.of(Map.of("key", "value"))));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace("""
            {
                "headers": {"key": "value"}
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_WritesEmptyObject_WhenBothUndefined() throws IOException {
        var settings = create(StatefulValue.undefined(), Headers.UNDEFINED_INSTANCE);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{}"));
    }

    protected abstract T create(StatefulValue<String> user, Headers headers);

    protected abstract T createFromMap(Map<String, Object> map, ConfigurationParseContext context);

    public static Map<String, Object> getOpenAiTaskSettingsMap(@Nullable String user) {
        var map = new HashMap<String, Object>();

        if (user != null) {
            map.put(OpenAiServiceFields.USER, user);
        }

        return map;
    }
}
