/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.common.xcontent;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 * Helper class to navigate nested objects using dot notation
 */
public final class ObjectPath {

    private static final String[] EMPTY_ARRAY = new String[0];

    private ObjectPath() {
    }

    /**
     * Return the value within a given object at the specified path, or
     * {@code null} if the path does not exist
     */
    public static <T> T eval(String path, Object object) {
        return (T) evalContext(path, object);
    }

    private static Object evalContext(String path, Object ctx) {
        final String[] parts;
        if (path == null || path.isEmpty()) parts = EMPTY_ARRAY;
        else parts = path.split("\\.");
        StringBuilder resolved = new StringBuilder();
        for (String part : parts) {
            if (ctx == null) {
                return null;
            }
            if (ctx instanceof Map) {
                ctx = ((Map) ctx).get(part);
                if (resolved.length() != 0) {
                    resolved.append(".");
                }
                resolved.append(part);
            } else if (ctx instanceof List) {
                try {
                    int index = Integer.parseInt(part);
                    ctx = ((List) ctx).get(index);
                    if (resolved.length() != 0) {
                        resolved.append(".");
                    }
                    resolved.append(part);
                } catch (NumberFormatException nfe) {
                    return null;
                }
            } else if (ctx.getClass().isArray()) {
                try {
                    int index = Integer.parseInt(part);
                    ctx = Array.get(ctx, index);
                    if (resolved.length() != 0) {
                        resolved.append(".");
                    }
                    resolved.append(part);
                } catch (NumberFormatException nfe) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return ctx;
    }
}
