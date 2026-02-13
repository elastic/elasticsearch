/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds the headers associated with a transform along with an optional UIAM token for cross-project authentication.
 */
public record TransformHeaders(Map<String, String> headers, @Nullable String uiamToken) implements Writeable, ToXContentFragment {

    public static final TransformHeaders EMPTY = new TransformHeaders(Map.of(), null);

    static final TransportVersion WITH_UIAM_TOKEN = TransportVersion.fromName("transform_with_uiam_token");

    private static final String UIAM_TOKEN_KEY = "_security_serverless_authenticating_token";

    public TransformHeaders {
        headers = headers != null ? Map.copyOf(headers) : Map.of();
    }

    public TransformHeaders(StreamInput in) throws IOException {
        this(readFromStream(in));
    }

    private TransformHeaders(TransformHeaders from) {
        this(from.headers, from.uiamToken);
    }

    private static TransformHeaders readFromStream(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(WITH_UIAM_TOKEN)) {
            var headers = in.readMap(StreamInput::readString);
            var uiamToken = in.readOptionalString();
            return new TransformHeaders(headers, uiamToken);
        }
        return fromMap(in.readMap(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(headers, StreamOutput::writeString);
        if (out.getTransportVersion().supports(WITH_UIAM_TOKEN)) {
            out.writeOptionalString(uiamToken);
        }
    }

    /**
     * Returns all headers as a single map, including the UIAM token if present.
     */
    public Map<String, String> allHeaders() {
        if (uiamToken == null) {
            return headers;
        }
        Map<String, String> combined = new HashMap<>(headers);
        combined.put(UIAM_TOKEN_KEY, uiamToken);
        return Map.copyOf(combined);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final boolean excludeGenerated = params.paramAsBoolean(TransformField.EXCLUDE_GENERATED, false);
        if (excludeGenerated) {
            return builder;
        }
        final boolean forInternalStorage = params.paramAsBoolean(TransformField.FOR_INTERNAL_STORAGE, false);
        Map<String, String> all = allHeaders();
        if (all.isEmpty() == false) {
            if (forInternalStorage) {
                builder.field(TransformConfig.HEADERS.getPreferredName(), all);
            } else {
                XContentUtils.addAuthorizationInfo(builder, headers);
            }
        }
        return builder;
    }

    @Override
    public String toString() {
        return "TransformHeaders{" + "headers=" + headers + ", hasUiamToken=" + (uiamToken != null) + '}';
    }

    public TransformHeaders withMintedInternalToken(String uiamToken) {
        if (TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled()) {
            return new TransformHeaders(headers, uiamToken);
        } else {
            return this;
        }
    }

    public static TransformHeaders fromMap(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return EMPTY;
        }
        var mutableHeaders = new HashMap<>(headers);
        var uiamToken = mutableHeaders.remove(UIAM_TOKEN_KEY);
        if (TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled() == false) {
            uiamToken = null;
        }
        return new TransformHeaders(mutableHeaders, uiamToken);
    }
}
