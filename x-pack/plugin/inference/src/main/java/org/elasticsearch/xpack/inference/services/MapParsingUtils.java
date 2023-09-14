/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;

import java.util.Map;

public class MapParsingUtils {
    /**
     * Remove the object from the map and cast to the expected type.
     * If the object cannot be cast to type an ElasticsearchStatusException
     * is thrown.
     *
     * @param sourceMap Map containing fields
     * @param key The key of the object to remove
     * @param type The expected type of the removed object
     * @return {@code null} if not present else the object cast to type T
     * @param <T> The expected type
     */
    @SuppressWarnings("unchecked")
    public static <T> T removeAsType(Map<String, Object> sourceMap, String key, Class<T> type) {
        Object o = sourceMap.remove(key);
        if (o == null) {
            return null;
        }

        if (type.isAssignableFrom(o.getClass())) {
            return (T) o;
        } else {
            throw new ElasticsearchStatusException(
                "field [{}] is not of the expected type." + " The value [{}] cannot be converted to a [{}]",
                RestStatus.BAD_REQUEST,
                key,
                o,
                type.getSimpleName()
            );
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> removeFromMapOrThrowIfNull(Map<String, Object> sourceMap, String fieldName) {
        Map<String, Object> value = (Map<String, Object>) sourceMap.remove(fieldName);
        if (value == null) {
            throw new ElasticsearchStatusException("Missing required field [{}]", RestStatus.BAD_REQUEST, fieldName);
        }
        return value;
    }

    public static ElasticsearchStatusException unknownSettingsError(Map<String, Object> config, String serviceName) {
        // TOOD map as JSON
        return new ElasticsearchStatusException(
            "Model configuration contains settings [{}] unknown to the [{}] service",
            RestStatus.BAD_REQUEST,
            config,
            serviceName
        );
    }

    public static String missingSettingErrorMsg(String settingName, String scope) {
        return Strings.format("[%s] does not contain the required setting [%s]", scope, settingName);
    }
}
