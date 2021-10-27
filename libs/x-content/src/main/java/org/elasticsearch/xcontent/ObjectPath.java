/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 * Helper class to navigate nested objects using dot notation
 */
public final class ObjectPath {

    private static final String[] EMPTY_ARRAY = new String[0];

    private ObjectPath() {}

    /**
     * Return the value within a given object at the specified path, or
     * {@code null} if the path does not exist
     */
    @SuppressWarnings("unchecked")
    public static <T> T eval(String path, Object object) {
        return (T) evalContext(path, object);
    }

    private static Object evalContext(String path, Object ctx) {
        final String[] parts;
        if (path == null || path.isEmpty()) parts = EMPTY_ARRAY;
        else parts = path.split("\\.");
        for (String part : parts) {
            if (ctx == null) {
                return null;
            }
            if (ctx instanceof Map) {
                ctx = ((Map) ctx).get(part);
            } else if (ctx instanceof List) {
                try {
                    int index = Integer.parseInt(part);
                    ctx = ((List) ctx).get(index);
                } catch (NumberFormatException nfe) {
                    return null;
                }
            } else if (ctx.getClass().isArray()) {
                try {
                    int index = Integer.parseInt(part);
                    ctx = Array.get(ctx, index);
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
