/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;

import java.util.EnumSet;

public final class StreamingUnifiedChatCompletionResultsUtils {

    public static final String MODEL_FIELD = "model";
    public static final String OBJECT_FIELD = "object";
    public static final String USAGE_FIELD = "usage";
    public static final String INDEX_FIELD = "index";
    public static final String ID_FIELD = "id";
    public static final String FUNCTION_NAME_FIELD = "name";
    public static final String FUNCTION_ARGUMENTS_FIELD = "arguments";
    public static final String FUNCTION_FIELD = "function";
    public static final String CHOICES_FIELD = "choices";
    public static final String DELTA_FIELD = "delta";
    public static final String CONTENT_FIELD = "content";
    public static final String REFUSAL_FIELD = "refusal";
    public static final String ROLE_FIELD = "role";
    public static final String TOOL_CALLS_FIELD = "tool_calls";
    public static final String FINISH_REASON_FIELD = "finish_reason";
    public static final String COMPLETION_TOKENS_FIELD = "completion_tokens";
    public static final String TOTAL_TOKENS_FIELD = "total_tokens";
    public static final String PROMPT_TOKENS_FIELD = "prompt_tokens";
    public static final String PROMPT_TOKENS_DETAILS_FIELD = "prompt_tokens_details";
    public static final String CACHED_TOKENS_FIELD = "cached_tokens";
    public static final String TYPE_FIELD = "type";

    public static final String NAME_FIELD = "name";
    public static final String TOP_P_FIELD = "top_p";
    public static final String TEMPERATURE_FIELD = "temperature";
    public static final String TEXT_FIELD = "text";
    public static final String REASONING_FIELD = "reasoning";
    public static final String REASONING_DETAILS_FIELD = "reasoning_details";
    public static final String SUMMARY_FIELD = "summary";
    public static final String REASONING_DETAIL_TYPE_FIELD = "reasoning_detail_type";
    public static final String DATA_FIELD = "data";
    public static final String FORMAT_FIELD = "format";
    public static final String SIGNATURE_FIELD = "signature";

    public static final String COMPLETION_TOKENS_DETAILS_FIELD = "completion_tokens_details";
    public static final String REASONING_TOKENS_FIELD = "reasoning_tokens";

    public static final TransportVersion INFERENCE_CACHED_TOKENS = TransportVersion.fromName("inference_cached_tokens");

    public static final TransportVersion CHAT_COMPLETION_REASONING_SUPPORT_ADDED = TransportVersion.fromName(
        "inference_api_chat_completion_reasoning_added"
    );

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

    private StreamingUnifiedChatCompletionResultsUtils() {}
}
