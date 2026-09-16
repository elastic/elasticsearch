/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Reports whether a service can serve a chat completion request with {@code stream} set to {@code false}, mirroring
 * {@code InferenceService#supportsNonStreamingChatCompletion()} so that callers can discover the capability through
 * {@code GET _inference/_services} without issuing a request.
 */
public class NonStreamingChatFeature extends SupportedInferenceFeature {
    public static final String NAME = "non_streaming_chat";

    public static final NonStreamingChatFeature SUPPORTED_INSTANCE = new NonStreamingChatFeature(true);
    public static final NonStreamingChatFeature UNSUPPORTED_INSTANCE = new NonStreamingChatFeature(false);

    private static final ObjectParser<Builder, Void> PARSER = buildCommonParser(NAME, Builder::new);

    /**
     * For a value decided at runtime. Prefer {@link #SUPPORTED_INSTANCE} / {@link #UNSUPPORTED_INSTANCE} when the
     * answer is known statically, which is the case for every service that declares this feature today.
     */
    public static NonStreamingChatFeature of(boolean supported) {
        return new NonStreamingChatFeature(supported);
    }

    public static NonStreamingChatFeature fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }

    public static class Builder extends SupportedInferenceFeature.Builder<NonStreamingChatFeature> {
        @Override
        protected NonStreamingChatFeature build(boolean supported) {
            return of(supported);
        }
    }

    private NonStreamingChatFeature(boolean supported) {
        super(supported);
    }

    public NonStreamingChatFeature(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
