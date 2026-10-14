/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.XContentFormattedException;

/**
 * Translates failures from the AWS Bedrock SDK into Elasticsearch exceptions that carry the upstream HTTP status.
 * <p>
 * Without this, a Bedrock 4xx (for example a 400 for an invalid prompt or a 429 for throttling) would reach the inference API
 * caller as a generic 500. Bedrock status codes are propagated verbatim, including 5xx, because they describe the upstream
 * failure accurately and nothing on the Bedrock path retries based on the mapped status. This intentionally differs from the
 * HTTP-based services, which fold unknown and 5xx upstream codes into a 400.
 * <p>
 * The AWS async client reports failures wrapped in {@link java.util.concurrent.CompletionException}, and the inference code wraps
 * exceptions again at several layers, so {@link #wrap} looks through the whole cause chain. It never downgrades an exception that
 * already carries a status ({@link ElasticsearchStatusException} or {@link XContentFormattedException}): such exceptions are
 * returned as-is so that, for example, a {@link org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException}
 * built by a streaming processor keeps its status, type and code all the way to the client.
 */
public final class AmazonBedrockExceptions {

    private AmazonBedrockExceptions() {}

    /**
     * Wraps the Bedrock exception preserving the upstream HTTP status, so that a Bedrock 4xx is not reported as a 500.
     */
    public static ElasticsearchStatusException fromBedrockException(String message, BedrockRuntimeException cause) {
        var status = RestStatus.fromCode(cause.statusCode());
        return new ElasticsearchStatusException(message, status != null ? status : RestStatus.INTERNAL_SERVER_ERROR, cause);
    }

    /**
     * Converts the throwable into an {@link ElasticsearchException} without losing a status that is already known.
     * <ul>
     *   <li>If the throwable is, or is caused by, an {@link ElasticsearchStatusException} or {@link XContentFormattedException}
     *       it is returned unchanged.</li>
     *   <li>If it is, or is caused by, a {@link BedrockRuntimeException} the Bedrock status is preserved via
     *       {@link #fromBedrockException}.</li>
     *   <li>Otherwise a plain {@link ElasticsearchException} (status 500) is returned.</li>
     * </ul>
     */
    public static ElasticsearchException wrap(String message, Throwable t) {
        if (ExceptionsHelper.unwrap(
            t,
            ElasticsearchStatusException.class,
            XContentFormattedException.class
        ) instanceof ElasticsearchException e) {
            return e;
        }
        if (ExceptionsHelper.unwrap(t, BedrockRuntimeException.class) instanceof BedrockRuntimeException bedrockException) {
            return fromBedrockException(message, bedrockException);
        }
        return new ElasticsearchException(message, t);
    }
}
