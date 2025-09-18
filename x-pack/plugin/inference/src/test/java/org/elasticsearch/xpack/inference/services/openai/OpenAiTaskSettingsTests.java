/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public abstract class OpenAiTaskSettingsTests<T extends OpenAiTaskSettings<T>> extends AbstractBWCWireSerializationTestCase<T> {

    public T createRandom() {
        return create(
            randomBoolean() ? null : randomAlphaOfLength(15),
            randomBoolean() ? null : Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15))
        );
    }

    public void testIsEmpty() {
        var randomSettings = create(randomBoolean() ? null : "username", randomBoolean() ? null : Map.of("key", "value"));
        var stringRep = Strings.toString(randomSettings);

        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();

        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.user() != null) {
            newSettingsMap.put(OpenAiServiceFields.USER, newSettings.user());
        }

        if (newSettings.headers() != null && newSettings.headers().isEmpty() == false) {
            newSettingsMap.put(OpenAiServiceFields.HEADERS, newSettings.headers());
        }

        var updatedSettings = initialSettings.updatedTaskSettings(Collections.unmodifiableMap(newSettingsMap));

        if (newSettings.user() == null) {
            assertEquals(initialSettings.user(), updatedSettings.user());
        } else {
            assertEquals(newSettings.user(), updatedSettings.user());
        }

        if (newSettings.headers() == null) {
            assertEquals(initialSettings.headers(), updatedSettings.headers());
        } else {
            assertEquals(newSettings.headers(), updatedSettings.headers());
        }
    }

    public void testFromMap_WithUserAndHeaders() {
        assertThat(
            create(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user", OpenAiServiceFields.HEADERS, Map.of("key", "value")))),
            is(create("user", Map.of("key", "value")))
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(ValidationException.class, () -> create(new HashMap<>(Map.of(OpenAiServiceFields.USER, ""))));

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = create(new HashMap<>(Map.of()));
        assertNull(taskSettings.user());
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        var settings = create(new HashMap<>(Map.of("key", "value")));
        assertNull(settings.user());
        assertNull(settings.headers());
    }

    public void testOf_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = create(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")));

        assertThat(taskSettings.updatedTaskSettings(Map.of()), is(taskSettings));
    }

    public void testOf_UsesOverriddenSettings() {
        var taskSettings = create(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")));

        assertThat(taskSettings.updatedTaskSettings(Map.of(OpenAiServiceFields.USER, "user2")), is(create("user2", null)));
    }

    public void testOf_UsesOverriddenSettings_ForHeaders() {
        var user = "user";
        var taskSettings = create(new HashMap<>(Map.of(OpenAiServiceFields.USER, user)));

        var headers = Map.of("key", "value");
        assertThat(taskSettings.updatedTaskSettings(Map.of(OpenAiServiceFields.HEADERS, headers)), is(create(user, headers)));
    }

    public void testFromMap_ParsesCorrectly_WhenUserIsNull() {
        var settings = create(new HashMap<>(Map.of(OpenAiServiceFields.HEADERS, new HashMap<>(Map.of("key", "value")))));

        assertNull(settings.user());
        assertThat(settings.headers(), is(Map.of("key", "value")));
    }

    public void testFromMap_ParsesCorrectly_WhenHeadersIsNull() {
        var settings = create(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")));

        assertThat(settings.user(), is("user"));
        assertNull(settings.headers());
    }

    @Override
    protected T mutateInstance(T instance) throws IOException {
        var setNull = randomBoolean();
        var fieldToMutate = randomIntBetween(0, 1);

        return switch (fieldToMutate) {
            case 0 -> create(
                instance.user() == null ? randomAlphaOfLength(15) : (setNull ? null : instance.user() + "modified"),
                instance.headers()
            );
            case 1 -> {
                if (instance.headers() == null) {
                    yield create(instance.user(), Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15)));
                } else if (setNull) {
                    yield create(instance.user(), null);
                } else {
                    var instanceHeaders = new HashMap<>(instance.headers() == null ? Map.of() : instance.headers());
                    instanceHeaders.put(randomAlphaOfLength(15), randomAlphaOfLength(15));
                    yield create(instance.user(), instanceHeaders);
                }
            }
            default -> throw new IllegalStateException("Unexpected value: " + fieldToMutate);
        };
    }

    protected abstract T create(@Nullable String user, @Nullable Map<String, String> headers);

    protected abstract T create(Map<String, Object> map);

    public static Map<String, Object> getOpenAiTaskSettingsMap(@Nullable String user) {
        var map = new HashMap<String, Object>();

        if (user != null) {
            map.put(OpenAiServiceFields.USER, user);
        }

        return map;
    }
}
