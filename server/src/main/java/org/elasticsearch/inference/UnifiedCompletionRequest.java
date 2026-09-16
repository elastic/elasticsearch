/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * The parameters for a {@link TaskType#CHAT_COMPLETION} request: the parsed request body along with whether the caller wants the
 * response streamed.
 * <br>
 * {@code stream} intentionally does not live on {@link UnifiedCompletionRequestBody}. It is determined by the REST route the request
 * arrived on ({@code _stream} or not) rather than by the JSON body, so it is never parsed by
 * {@link UnifiedCompletionRequestBody#PARSER}.
 *
 * @param body   The parsed chat completion request body
 * @param stream Whether the response should be streamed back to the caller
 */
public record UnifiedCompletionRequest(UnifiedCompletionRequestBody body, boolean stream) implements Writeable {

    public UnifiedCompletionRequest {
        Objects.requireNonNull(body);
    }

    public UnifiedCompletionRequest(StreamInput in) throws IOException {
        this(new UnifiedCompletionRequestBody(in), in.readBoolean());
    }

    /**
     * Creates a request that streams the response. Useful for the code paths that only support streaming, like validating that an
     * endpoint's configuration works.
     */
    public static UnifiedCompletionRequest streaming(UnifiedCompletionRequestBody body) {
        return new UnifiedCompletionRequest(body, true);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        body.writeTo(out);
        out.writeBoolean(stream);
    }
}
