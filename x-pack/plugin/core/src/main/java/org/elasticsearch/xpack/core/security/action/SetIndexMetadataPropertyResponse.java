/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.action.SetIndexMetadataPropertyRequest.readOptionalStringMap;
import static org.elasticsearch.xpack.core.security.action.SetIndexMetadataPropertyRequest.writeOptionalStringMap;

public class SetIndexMetadataPropertyResponse extends ActionResponse {
    private final String key;
    @Nullable
    private final Map<String, String> value;

    public SetIndexMetadataPropertyResponse(String key, @Nullable Map<String, String> value) {
        this.key = key;
        this.value = value;
    }

    public SetIndexMetadataPropertyResponse(StreamInput in) throws IOException {
        key = in.readString();
        value = readOptionalStringMap(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key);
        writeOptionalStringMap(value, out);
    }
}
