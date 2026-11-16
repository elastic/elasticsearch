/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public enum Nullability {
    /**
     * Whether the expression can become null
     */
    TRUE,

    /**
     * The expression can never become null
     */
    FALSE,

    /**
     * Cannot determine if the expression supports possible null folding
     */
    UNKNOWN;

    /**
     * Newer versions removed {@link #UNKNOWN} and serialize nullability as a boolean. This branch keeps
     * {@link #UNKNOWN} but must speak the same wire format, because the patch id reserved for this
     * branch makes newer nodes assume it is supported.
     */
    public static final TransportVersion ESQL_NULLABILITY_REMOVE_UNKNOWN = TransportVersion.fromName("esql_nullability_remove_unknown");

    /**
     * Reads a {@link Nullability} from the stream. Senders on or after {@link #ESQL_NULLABILITY_REMOVE_UNKNOWN}
     * only send {@link #TRUE} or {@link #FALSE}.
     */
    public static Nullability readFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(ESQL_NULLABILITY_REMOVE_UNKNOWN)) {
            return in.readBoolean() ? TRUE : FALSE;
        } else {
            return in.readEnum(Nullability.class);
        }
    }

    /**
     * Writes this {@link Nullability} to the stream. {@link #UNKNOWN} is sent as {@link #TRUE} to receivers on or
     * after {@link #ESQL_NULLABILITY_REMOVE_UNKNOWN}, the same as newer versions map a legacy {@link #UNKNOWN}.
     */
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_NULLABILITY_REMOVE_UNKNOWN)) {
            out.writeBoolean(this != FALSE);
        } else {
            out.writeEnum(this);
        }
    }
}
