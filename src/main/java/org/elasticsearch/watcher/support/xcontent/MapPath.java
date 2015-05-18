/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.xcontent;

import org.elasticsearch.common.Strings;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MapPath {

    private MapPath() {
    }

    public static <T> T eval(String path, Map<String, Object> map) {
        return (T) eval(path, (Object) map);
    }

    private static Object eval(String path, Object ctx) {
        String[] parts = Strings.splitStringToArray(path, '.');
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
