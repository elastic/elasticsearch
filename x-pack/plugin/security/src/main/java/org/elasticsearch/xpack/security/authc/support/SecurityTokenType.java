/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Represents the different "types" of tokens that Elasticsearch works with.
 * Some (but not all) of these may be used in an <code>Authorization: Bearer {value}</code> header.
 */
public enum SecurityTokenType {

    // There enum values are written to streams. They cannot be reordered
    ACCESS_TOKEN,
    REFRESH_TOKEN,
    SERVICE_ACCOUNT;

    public void write(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static SecurityTokenType read(StreamInput in) throws IOException {
        return in.readEnum(SecurityTokenType.class);
    }
}
