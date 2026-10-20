/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.core.inference.results.XContentFormattedException;

/**
 * The single definition of the error shape that the inference REST APIs return.
 * <p>
 * An error that the inference layer has already formatted — an {@link XContentFormattedException}, most notably its
 * {@link UnifiedChatCompletionException} subclass which renders the OpenAI-compatible
 * <code>{"error":{"code","message","param","type"}}</code> body — must be rendered with its own XContent instead of the standard
 * {@link ElasticsearchException#generateFailureXContent} envelope. Both {@link ServerSentEventsRestActionListener}
 * (the <code>_stream</code> routes) and {@link RestInferenceAction} (the non-streaming routes) consult this class, so that the error
 * shape a caller sees does not depend on whether they asked for a stream.
 */
final class InferenceErrorFormat {

    /**
     * @return the pre-formatted exception at the root of {@code t}, or {@code null} when the inference layer did not format it and the
     *         caller should fall back to the standard error envelope.
     */
    @Nullable
    static XContentFormattedException formattedException(Throwable t) {
        return ExceptionsHelper.unwrapCause(t) instanceof XContentFormattedException xContentFormattedException
            ? xContentFormattedException
            : null;
    }

    private InferenceErrorFormat() {}
}
