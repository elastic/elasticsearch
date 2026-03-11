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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;

import java.util.EnumSet;
import java.util.Map;

public final class UnifiedCompletionUtils {

    public static final String ID_FIELD = "id";
    public static final String TYPE_FIELD = "type";
    public static final String MODEL_FIELD = "model";
    public static final String INDEX_FIELD = "index";
    public static final String OBJECT_FIELD = "object";

    public static final String MESSAGES_FIELD = "messages";
    public static final String ROLE_FIELD = "role";
    public static final String CONTENT_FIELD = "content";
    public static final String CHOICES_FIELD = "choices";
    public static final String DELTA_FIELD = "delta";
    public static final String REFUSAL_FIELD = "refusal";

    public static final String TOOL_CALL_ID_FIELD = "tool_call_id";
    public static final String TOOL_CALLS_FIELD = "tool_calls";
    public static final String FUNCTION_FIELD = "function";
    public static final String ARGUMENTS_FIELD = "arguments";
    public static final String TOOL_CHOICE_FIELD = "tool_choice";
    public static final String TOOL_FIELD = "tools";
    public static final String FUNCTION_NAME_FIELD = "name";
    public static final String FUNCTION_ARGUMENTS_FIELD = "arguments";
    public static final String DESCRIPTION_FIELD = "description";
    public static final String NAME_FIELD = "name";

    public static final String PARAMETERS_FIELD = "parameters";
    public static final String STRICT_FIELD = "strict";
    public static final String TOP_P_FIELD = "top_p";
    public static final String STOP_FIELD = "stop";
    public static final String TEMPERATURE_FIELD = "temperature";
    public static final String MAX_COMPLETION_TOKENS_FIELD = "max_completion_tokens";
    public static final String MAX_TOKENS_FIELD = "max_tokens";

    public static final String IMAGE_URL_FIELD = "image_url";
    public static final String URL_FIELD = "url";
    public static final String DETAIL_FIELD = "detail";
    public static final String FILE_FIELD = "file";
    public static final String FILE_DATA_FIELD = "file_data";
    public static final String FILE_ID_FIELD = "file_id";
    public static final String FILENAME_FIELD = "filename";
    public static final String TEXT_FIELD = "text";

    public static final String REASONING_FIELD = "reasoning";
    public static final String REASONING_DETAILS_FIELD = "reasoning_details";
    public static final String EFFORT_FIELD = "effort";
    public static final String SUMMARY_FIELD = "summary";
    public static final String EXCLUDE_FIELD = "exclude";
    public static final String ENABLED_FIELD = "enabled";
    public static final String REASONING_DETAIL_TYPE_FIELD = "reasoning_detail_type";
    public static final String FORMAT_FIELD = "format";
    public static final String SIGNATURE_FIELD = "signature";
    public static final String DATA_FIELD = "data";

    public static final String USAGE_FIELD = "usage";
    public static final String FINISH_REASON_FIELD = "finish_reason";
    public static final String COMPLETION_TOKENS_FIELD = "completion_tokens";
    public static final String TOTAL_TOKENS_FIELD = "total_tokens";
    public static final String PROMPT_TOKENS_FIELD = "prompt_tokens";
    public static final String PROMPT_TOKENS_DETAILS_FIELD = "prompt_tokens_details";
    public static final String CACHED_TOKENS_FIELD = "cached_tokens";
    public static final String COMPLETION_TOKENS_DETAILS_FIELD = "completion_tokens_details";
    public static final String REASONING_TOKENS_FIELD = "reasoning_tokens";

    public static final TransportVersion INFERENCE_CACHED_TOKENS = TransportVersion.fromName("inference_cached_tokens");

    public static final TransportVersion MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED = TransportVersion.fromName(
        "inference_api_multimodal_chat_completion"
    );

    public static final TransportVersion CHAT_COMPLETION_REASONING_SUPPORT_ADDED = TransportVersion.fromName(
        "inference_api_chat_completion_reasoning_added"
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

    public static void validateNonNegativeLong(@Nullable Long index, String fieldName) {
        if (index != null && index < 0) {
            throw new ElasticsearchStatusException(
                Strings.format("Field [%s] must be non-negative, but was [%d]", fieldName, index),
                RestStatus.BAD_REQUEST
            );
        }
    }

    private UnifiedCompletionUtils() {}
}
