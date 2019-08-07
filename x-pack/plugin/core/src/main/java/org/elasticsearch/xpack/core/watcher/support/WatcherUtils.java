/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils.formatDate;

public final class WatcherUtils {

    private static final Pattern NO_WS_PATTERN = Pattern.compile("\\S+");

    private WatcherUtils() {
    }

    public static Map<String, Object> responseToData(ToXContentObject response, ToXContent.Params params) throws IOException {
        return XContentHelper.convertToMap(XContentHelper.toXContent(response, XContentType.JSON, params, false), false,
            XContentType.JSON).v2();
    }

    public static Map<String, Object> flattenModel(Map<String, Object> map) {
        Map<String, Object> result = new HashMap<>();
        flattenModel("", map, result);
        return result;
    }

    private static void flattenModel(String key, Object value, Map<String, Object> result) {
        if (value == null) {
            result.put(key, null);
            return;
        }
        if (value instanceof Map) {
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                if ("".equals(key)) {
                    flattenModel(entry.getKey(), entry.getValue(), result);
                } else {
                    flattenModel(key + "." + entry.getKey(), entry.getValue(), result);
                }
            }
            return;
        }
        if (value instanceof Iterable) {
            int i = 0;
            for (Object item : (Iterable) value) {
                flattenModel(key + "." + i++, item, result);
            }
            return;
        }
        if (value.getClass().isArray()) {
            for (int i = 0; i < Array.getLength(value); i++) {
                flattenModel(key + "." + i, Array.get(value, i), result);
            }
            return;
        }
        if (value instanceof ZonedDateTime) {
            result.put(key, formatDate((ZonedDateTime) value));
            return;
        }
        if (value instanceof TimeValue) {
            result.put(key, String.valueOf(((TimeValue) value).getMillis()));
            return;
        }
        result.put(key, String.valueOf(value));
    }

    public static boolean isValidId(String id) {
        return Strings.isEmpty(id) == false && NO_WS_PATTERN.matcher(id).matches();
    }
}
