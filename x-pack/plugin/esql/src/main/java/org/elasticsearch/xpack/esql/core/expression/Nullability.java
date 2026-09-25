/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public enum Nullability {
    // TODO: remove Nullability by a simple boolean.
    // Since this touches a lot of files, do it in a separate PR.

    /**
     * Whether the expression can become null
     */
    TRUE,

    /**
     * The expression can never become null
     */
    FALSE;

    public static final TransportVersion ESQL_NULLABILITY_REMOVE_UNKNOWN = TransportVersion.fromName("esql_nullability_remove_unknown");

    /**
     * Reads a {@link Nullability} from the stream, remapping the legacy UNKNOWN ordinal (2) to TRUE
     * when the sender predates {@link #ESQL_NULLABILITY_REMOVE_UNKNOWN}.
     */
    public static Nullability readFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(ESQL_NULLABILITY_REMOVE_UNKNOWN)) {
            return in.readEnum(Nullability.class);
        } else {
            try {
                return in.readEnum(Nullability.class);
            } catch (IOException e) {
                // remap legacy UNKNOWN to TRUE for senders that predate ESQL_NULLABILITY_REMOVE_UNKNOWN
                return TRUE;
            }
        }
    }
}
