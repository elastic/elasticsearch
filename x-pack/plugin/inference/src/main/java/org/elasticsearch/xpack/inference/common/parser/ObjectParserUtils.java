/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;

import java.util.Map;

public final class ObjectParserUtils {

    /**
     * Remove the object from the map and cast to the expected type.
     * If the object cannot be cast to type an {@link IllegalArgumentException}
     * is thrown.
     *
     * @param sourceMap Map containing fields
     * @param key The key of the object to remove
     * @param type The expected type of the removed object
     * @return {@code null} if not present else the object cast to type T
     * @param <T> The expected type
     */
    @SuppressWarnings("unchecked")
    public static <T> T removeAsType(Map<String, Object> sourceMap, String key, String root, Class<T> type) {
        Object o = sourceMap.remove(key);
        if (o == null) {
            return null;
        }

        if (type.isAssignableFrom(o.getClass())) {
            return (T) o;
        } else {
            throw new IllegalArgumentException(invalidTypeErrorMsg(key, root, o, type.getSimpleName()));
        }
    }

    public static String invalidTypeErrorMsg(String settingName, String root, Object foundObject, String expectedType) {
        return Strings.format(
            "field [%s] is not of the expected type. The value [%s] cannot be converted to a [%s]",
            pathToKey(root, settingName),
            foundObject,
            expectedType
        );
    }

    public static String pathToKey(String root, String... properties) {
        if (properties == null || properties.length == 0) {
            return root;
        }

        var subPath = String.join(".", properties);

        if (root == null || root.isEmpty()) {
            return subPath;
        }

        return Strings.format("%s.%s", root, subPath);
    }

    public static boolean isMapNullOrEmpty(@Nullable Map<String, Object> map) {
        return map == null || map.isEmpty();
    }

    private ObjectParserUtils() {}
}
