/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.HeadersTests;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettings.INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public abstract class AzureOpenAiTaskSettingsTests<T extends AzureOpenAiTaskSettings<T>> extends AbstractBWCWireSerializationTestCase<T> {

    private static final String USER = "user";
    private static final Map<String, String> HEADERS_MAP = Map.of("key", "value");
    private static final Headers HEADERS = new Headers(StatefulValue.of(HEADERS_MAP));

    public T createRandom() {
        var user = randomUser();
        var headers = HeadersTests.createRandom();
        return create(user, headers);
    }

    private String randomUser() {
        return randomBoolean() ? null : randomAlphaOfLength(15);
    }

    public T createRandomWithUser() {
        return create(randomAlphaOfLength(15), null);
    }

    public void testIsEmpty() {
        var bothNull = create(null, null);
        assertTrue(bothNull.isEmpty());

        var nullUserEmptyHeaders = create(null, new Headers(StatefulValue.of(Map.of())));
        assertTrue(nullUserEmptyHeaders.isEmpty());

        var nullHeaders = create(USER, null);
        assertFalse(nullHeaders.isEmpty());

        var nullUser = create(null, HEADERS);
        assertFalse(nullUser.isEmpty());

        var neitherNull = create(USER, HEADERS);
        assertFalse(neitherNull.isEmpty());
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();

        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.user().isPresent()) {
            newSettingsMap.put(AzureOpenAiServiceFields.USER, newSettings.user().get());
        }

        if (newSettings.headers().isPresent()) {
            newSettingsMap.put(Headers.HEADERS_FIELD, new HashMap<>(newSettings.headers().value().get()));
        }

        var updatedSettings = initialSettings.updatedTaskSettings(Collections.unmodifiableMap(newSettingsMap));

        if (newSettings.user().isPresent()) {
            assertEquals(newSettings.user(), updatedSettings.user());
        } else if (newSettings.user().isNull()) {
            // When the new settings has a null user, we want to remove the existing user, so the updated settings should now
            // have the user as undefined
            assertEquals(StatefulValue.undefined(), updatedSettings.user());
        } else {
            // If the new settings did not have user, the updated settings should keep the existing user
            assertEquals(initialSettings.user(), updatedSettings.user());
        }

        if (newSettings.headers().isPresent()) {
            assertEquals(newSettings.headers(), updatedSettings.headers());
        } else if (newSettings.headers().isNull()) {
            // When the new settings has a null headers field, we want to remove the existing headers, so the updated settings should now
            // have the headers as undefined
            assertEquals(Headers.UNDEFINED_INSTANCE, updatedSettings.headers());
        } else {
            // If the new settings did not have the headers field, the updated settings should keep the existing headers
            assertEquals(initialSettings.headers(), updatedSettings.headers());
        }
    }

    public void testUpdatedTaskSettings_ApplyingEmptyHeaders() {
        var initialSettingsNullHeaders = create(USER, null);
        Map<String, Object> newSettingsMap = Map.of(Headers.HEADERS_FIELD, Map.of());

        var updatedSettings = initialSettingsNullHeaders.updatedTaskSettings(newSettingsMap);
        assertThat(updatedSettings, is(create(USER, null)));

        var initialSettingsDefinedHeaders = create(USER, HEADERS);
        updatedSettings = initialSettingsDefinedHeaders.updatedTaskSettings(newSettingsMap);
        assertThat(updatedSettings, is(initialSettingsDefinedHeaders));
    }

    public void testFromMap_WithUserAndHeaders() {
        assertThat(
            createFromMap(
                new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, USER, Headers.HEADERS_FIELD, HEADERS_MAP)),
                ConfigurationParseContext.REQUEST
            ),
            is(create(USER, HEADERS))
        );
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

    public void testFromMap_UserIsEmptyString_DoesNotThrowForPersistentContext() {
        var settings = createFromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, "")), ConfigurationParseContext.PERSISTENT);
        assertTrue(settings.user().isPresent() && settings.user().get().isEmpty());
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = createFromMap(new HashMap<>(Map.of()), ConfigurationParseContext.REQUEST);
        assertTrue(taskSettings.user().isUndefined());
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        // The HashMap is missing the headers key
        var settings = createFromMap(new HashMap<>(HEADERS_MAP), ConfigurationParseContext.PERSISTENT);
        assertTrue(settings.user().isUndefined());
        assertThat(settings.headers(), sameInstance(Headers.UNDEFINED_INSTANCE));
    }

    public void testFromMap_ParsesCorrectly_WhenUserIsMissing() {
        var settings = createFromMap(
            new HashMap<>(Map.of(Headers.HEADERS_FIELD, new HashMap<>(HEADERS_MAP))),
            ConfigurationParseContext.REQUEST
        );

        assertTrue(settings.user().isUndefined());
        assertThat(settings.headers(), is(HEADERS));
    }

    public void testFromMap_ParsesCorrectly_WhenHeadersIsMissing() {
        var settings = createFromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, USER)), ConfigurationParseContext.REQUEST);

        assertTrue(settings.user().isPresent());
        assertThat(settings.user().get(), is(USER));
        assertThat(settings.headers(), is(Headers.UNDEFINED_INSTANCE));
    }

    public void testFromMap_ParsesCorrectly_WhenHeadersIsEmptyMap() {
        var settings = createFromMap(
            new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, USER, Headers.HEADERS_FIELD, Map.of())),
            ConfigurationParseContext.REQUEST
        );

        assertTrue(settings.user().isPresent());
        assertThat(settings.user().get(), is(USER));
        assertTrue(settings.headers().isEmpty());
    }

    public void testFromMap_ParsesCorrectly_WhenHeadersMapOfNulls() {
        var headersMap = new HashMap<String, Object>();
        headersMap.put("key1", null);
        headersMap.put("key2", null);
        var settings = createFromMap(
            new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, USER, Headers.HEADERS_FIELD, headersMap)),
            ConfigurationParseContext.REQUEST
        );

        assertTrue(settings.user().isPresent());
        assertThat(settings.user().get(), is(USER));
        assertTrue(settings.headers().isEmpty());
    }

    public void testFromMap_ThrowsException_WhenHeadersContainsAnInteger() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> createFromMap(
                new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, USER, Headers.HEADERS_FIELD, new HashMap<>(Map.of("key", 1)))),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(exception.getMessage(), containsString("failed to parse field [headers]"));
        assertThat(exception.getCause().getMessage(), containsString(
            "Map field [headers] has an entry that is not "
                + "valid, [key => 1]. Value type of [Integer] is not one of [String].;"
        ));
    }

    public void testFromMap_ThrowsException_WhenUserIsAnInteger() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> createFromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, 1)), ConfigurationParseContext.REQUEST)
        );

        assertThat(
            exception.getMessage(),
            containsString("[azure_openai_task_settings_parser] user doesn't support values of type: VALUE_NUMBER")
        );
    }

    public void testFromMap_WithUser() {
        assertThat(
            create(USER, null),
            is(createFromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, USER)), ConfigurationParseContext.PERSISTENT))
        );
    }

    public void testFromMap_WithRequestContext_ReturnsEmptySettings_WhenMapIsEmpty() {
        var settings = createFromMap(new HashMap<>(Map.of()), ConfigurationParseContext.REQUEST);
        assertTrue(settings.isEmpty());
        assertTrue(settings.user().isUndefined());
        assertThat(settings.headers(), sameInstance(Headers.UNDEFINED_INSTANCE));
        assertThat(settings, sameInstance(emptySettings()));
    }

    public void testUpdatedTaskSettings_KeepsOriginalValues_WhenOverridesAreEmpty() {
        var taskSettings = createFromMap(
            new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, USER, Headers.HEADERS_FIELD, HEADERS_MAP)),
            ConfigurationParseContext.PERSISTENT
        );

        var overriddenTaskSettings = taskSettings.updatedTaskSettings(Map.of());
        assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testToXContent_RoundTrip() throws IOException {
        var user = randomUser();
        // The reason we don't allow null here is that when a Headers::NULL_INSTANCE is serialized to xContent
        // it is not written (aka would look like this {}) instead of it being written {"headers": null}.
        // This is because it's only used for the update API to indicate that the existing headers should be removed.
        var headers = HeadersTests.createRandomNonNull();
        var original = create(user, headers);

        String json;
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            json = Strings.toString(builder);
        }
        var map = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        var roundTrippedPersistentContext = createFromMap(map, ConfigurationParseContext.PERSISTENT);
        assertThat(roundTrippedPersistentContext, is(original));

        var roundTrippedRequestContext = createFromMap(map, ConfigurationParseContext.REQUEST);
        assertThat(roundTrippedRequestContext, is(original));
    }

    public void testFromMap_ThrowsException_WhenMapContainsExtraFields_ForRequestContext() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> createFromMap(
                new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, USER, Headers.HEADERS_FIELD, Map.of(), "extra_field", "value")),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(exception.getMessage(), containsString("[azure_openai_task_settings_parser] unknown field [extra_field]"));
    }

    public void testFromMap_DoesNotThrowException_WhenMapContainsExtraFields_ForPersistentContext() {
        var settings = createFromMap(
            new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, USER, Headers.HEADERS_FIELD, Map.of(), "extra_field", "value")),
            ConfigurationParseContext.PERSISTENT
        );

        assertTrue(settings.user().isPresent());
        assertThat(settings.user().get(), is(USER));
        assertTrue(settings.headers().isEmpty());
    }

    public static Map<String, Object> createRequestTaskSettingsMap(@Nullable String user) {
        var map = new HashMap<String, Object>();

        if (user != null) {
            map.put(AzureOpenAiServiceFields.USER, user);
        }

        return map;
    }

    @Override
    protected T mutateInstanceForVersion(T instance, TransportVersion version) {
        if (version.supports(INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS)) {
            return instance;
        }

        var user = instance.user().isPresent() ? instance.user().get() : null;
        return create(user, null);
    }

    @Override
    protected T mutateInstance(T instance) {
        var setNull = randomBoolean();
        var fieldToMutate = randomIntBetween(0, 1);

        var userForCreate = instance.user().isPresent() ? instance.user().get() : null;

        return switch (fieldToMutate) {
            case 0 -> create(
                userForCreate == null ? randomAlphaOfLength(15) : (setNull ? null : userForCreate + "modified"),
                instance.headers()
            );
            case 1 -> create(userForCreate, HeadersTests.doMutateInstance(instance.headers()));
            default -> throw new IllegalStateException("Unexpected value: " + fieldToMutate);
        };
    }

    protected abstract T create(@Nullable String user, @Nullable Headers headers);

    protected abstract T createFromMap(Map<String, Object> map, ConfigurationParseContext context);

    protected abstract T emptySettings();
}
