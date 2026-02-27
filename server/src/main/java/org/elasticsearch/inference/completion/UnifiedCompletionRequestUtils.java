/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;

import java.util.EnumSet;
import java.util.Map;

public final class UnifiedCompletionRequestUtils {

    public static final String NAME_FIELD = "name";
    public static final String TOOL_CALL_ID_FIELD = "tool_call_id";
    public static final String TOOL_CALLS_FIELD = "tool_calls";
    public static final String ID_FIELD = "id";
    public static final String FUNCTION_FIELD = "function";
    public static final String ARGUMENTS_FIELD = "arguments";
    public static final String DESCRIPTION_FIELD = "description";
    public static final String PARAMETERS_FIELD = "parameters";
    public static final String STRICT_FIELD = "strict";
    public static final String TOP_P_FIELD = "top_p";
    public static final String MESSAGES_FIELD = "messages";
    public static final String ROLE_FIELD = "role";
    public static final String CONTENT_FIELD = "content";
    public static final String STOP_FIELD = "stop";
    public static final String TEMPERATURE_FIELD = "temperature";
    public static final String TOOL_CHOICE_FIELD = "tool_choice";
    public static final String TOOL_FIELD = "tools";
    public static final String TEXT_FIELD = "text";
    public static final String IMAGE_URL_FIELD = "image_url";
    public static final String URL_FIELD = "url";
    public static final String DETAIL_FIELD = "detail";
    public static final String FILE_FIELD = "file";
    public static final String FILE_DATA_FIELD = "file_data";
    public static final String FILE_ID_FIELD = "file_id";
    public static final String FILENAME_FIELD = "filename";
    public static final String TYPE_FIELD = "type";
    public static final String MODEL_FIELD = "model";
    public static final String MAX_COMPLETION_TOKENS_FIELD = "max_completion_tokens";
    public static final String MAX_TOKENS_FIELD = "max_tokens";

    public static final TransportVersion MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED = TransportVersion.fromName(
        "inference_api_multimodal_chat_completion"
    );

    public static <T> T extractRequiredFieldOfType(Map<String, Object> sourceMap, String key, Class<T> type, String containingObject) {
        return extractFieldOfType(sourceMap, key, type, true, containingObject);
    }

    public static <T> T extractOptionalFieldOfType(Map<String, Object> sourceMap, String key, Class<T> type, String containingObject) {
        return extractFieldOfType(sourceMap, key, type, false, containingObject);
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractFieldOfType(
        Map<String, Object> sourceMap,
        String key,
        Class<T> type,
        boolean required,
        String containingObject
    ) {
        Object o = sourceMap.remove(key);
        if (o == null) {
            if (required) {
                throw new ElasticsearchStatusException(
                    Strings.format("Field [%s] in object [%s] is required but was not found", key, containingObject),
                    RestStatus.BAD_REQUEST
                );
            } else {
                return null;
            }
        }

        if (type.isAssignableFrom(o.getClass())) {
            return (T) o;
        } else {
            throw new ElasticsearchStatusException(
                Strings.format(
                    "Field [%s] in object [%s] is not of the expected type. Expected [%s] but was [%s]",
                    key,
                    containingObject,
                    type.getSimpleName(),
                    o.getClass().getSimpleName()
                ),
                RestStatus.BAD_REQUEST
            );
        }
    }

    public static void throwIfNotEmptyMap(Map<String, Object> map, String containingObject) {
        if (map != null && map.isEmpty() == false) {
            throw new ElasticsearchStatusException(
                Strings.format("[%s] contains unknown fields %s", containingObject, map.keySet()),
                RestStatus.BAD_REQUEST
            );
        }
    }

    public static <E extends Enum<E>> ElasticsearchStatusException getUnrecognizedTypeException(
        String name,
        String containingObject,
        Class<E> clazz
    ) {
        return new ElasticsearchStatusException(
            Strings.format("Unrecognized type [%s] in object [%s], must be one of %s", name, containingObject, EnumSet.allOf(clazz)),
            RestStatus.BAD_REQUEST
        );
    }

    private UnifiedCompletionRequestUtils() {}
}
