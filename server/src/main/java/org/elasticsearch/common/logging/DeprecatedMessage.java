/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A logger message used by {@link DeprecationLogger}.
 * Carries x-opaque-id field if provided in the headers. Will populate the x-opaque-id field in JSON logs.
 */
public class DeprecatedMessage extends ESLogMessage {
    public static final String X_OPAQUE_ID_FIELD_NAME = "x-opaque-id";
    public static final String KEY_FIELD_NAME = "key";

    public DeprecatedMessage(DeprecationCategory category, String key, String xOpaqueId, String messagePattern, Object... args) {
        super(fieldMap(category, key, xOpaqueId), messagePattern, args);
    }

    private static Map<String, Object> fieldMap(DeprecationCategory category, String key, String xOpaqueId) {
        final MapBuilder<String, Object> builder = MapBuilder.newMapBuilder();

        // The fields below are emitted using ECS keys in `EcsJsonLayout`

        Objects.requireNonNull(category, "category cannot be null");
        builder.put("category", category.name().toLowerCase(Locale.ROOT));

        if (Strings.isNullOrEmpty(key) == false) {
            builder.put(KEY_FIELD_NAME, key);
        }
        if (Strings.isNullOrEmpty(xOpaqueId) == false) {
            builder.put(X_OPAQUE_ID_FIELD_NAME, xOpaqueId);
        }
        return builder.immutableMap();
    }
}
