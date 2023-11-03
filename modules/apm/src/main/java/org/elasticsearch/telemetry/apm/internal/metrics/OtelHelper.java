/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.common.Attributes;

import java.util.Map;

class OtelHelper {
    static Attributes fromMap(Map<String, Object> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return Attributes.empty();
        }
        var builder = Attributes.builder();
        attributes.forEach((k, v) -> {
            if (v instanceof String value) {
                builder.put(k, value);
            } else if (v instanceof Long value) {
                builder.put(k, value);
            } else if (v instanceof Integer value) {
                builder.put(k, value);
            } else if (v instanceof Byte value) {
                builder.put(k, value);
            } else if (v instanceof Short value) {
                builder.put(k, value);
            } else if (v instanceof Double value) {
                builder.put(k, value);
            } else if (v instanceof Float value) {
                builder.put(k, value);
            } else if (v instanceof Boolean value) {
                builder.put(k, value);
            } else {
                throw new IllegalArgumentException("attributes do not support value type of [" + v.getClass().getCanonicalName() + "]");
            }
        });
        return builder.build();
    }
}
