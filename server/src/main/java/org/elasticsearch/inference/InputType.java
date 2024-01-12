/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Defines the type of request, whether the request is to ingest a document or search for a document.
 */
public enum InputType implements Writeable {
    INGEST,
    SEARCH;

    public static String NAME = "input_type";

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static InputType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static InputType fromStream(StreamInput in) throws IOException {
        return in.readOptionalEnum(InputType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(this);
    }
}
