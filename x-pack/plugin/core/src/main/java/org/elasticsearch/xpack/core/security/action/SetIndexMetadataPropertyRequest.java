/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SetIndexMetadataPropertyRequest extends MasterNodeRequest<SetIndexMetadataPropertyRequest> implements IndicesRequest {
    private final String index;
    private final String key;
    @Nullable
    private final Map<String, String> expected;
    @Nullable
    private final Map<String, String> value;

    public SetIndexMetadataPropertyRequest(
        TimeValue timeout,
        String index,
        String key,
        @Nullable Map<String, String> expected,
        @Nullable Map<String, String> value
    ) {
        super(timeout);
        this.index = Objects.requireNonNull(index);
        this.key = Objects.requireNonNull(key);
        this.expected = expected;
        this.value = value;
    }

    protected SetIndexMetadataPropertyRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.key = in.readString();
        this.expected = readOptionalStringMap(in);
        this.value = readOptionalStringMap(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeString(key);
        writeOptionalStringMap(expected, out);
        writeOptionalStringMap(value, out);
    }

    static void writeOptionalStringMap(@Nullable Map<String, String> map, StreamOutput out) throws IOException {
        if (map == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(map, StreamOutput::writeString);
        }
    }

    static Map<String, String> readOptionalStringMap(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return in.readImmutableMap(StreamInput::readString);
        } else {
            return null;
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String[] indices() {
        return new String[] { index };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    public String index() {
        return index;
    }

    public String key() {
        return key;
    }

    public @Nullable Map<String, String> expected() {
        return expected;
    }

    public @Nullable Map<String, String> value() {
        return value;
    }
}
