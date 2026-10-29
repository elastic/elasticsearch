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
    // TODO: remove Nullability enum and replace by a simple boolean.
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

    // 9.4 and 8.19 have patch ids reserved, but backporting "[ES|QL] remove nullability unknown (#160460)"
    // (https://github.com/elastic/elasticsearch/pull/160460) never happened due to too many merge conflicts.
    // Therefore, keep sending the legacy wire format to those patch versions.
    private static final TransportVersion UNUSED_9_4_PATCH = new TransportVersion(9348024);
    private static final TransportVersion UNUSED_8_19_PATCH = new TransportVersion(8841107);

    static boolean usesBooleanFormat(TransportVersion version) {
        return version.supports(ESQL_NULLABILITY_REMOVE_UNKNOWN)
            && version.isPatchFrom(UNUSED_9_4_PATCH) == false
            && version.isPatchFrom(UNUSED_8_19_PATCH) == false;
    }

    /**
     * Reads a {@link Nullability} from the stream, remapping the legacy UNKNOWN ordinal (2) to TRUE
     * when the sender doesn't use the boolean format.
     */
    public static Nullability readFrom(StreamInput in) throws IOException {
        if (usesBooleanFormat(in.getTransportVersion())) {
            return in.readBoolean() ? TRUE : FALSE;
        } else {
            int ordinal = in.readVInt();
            return switch (ordinal) {
                case 0 -> TRUE;
                case 1 -> FALSE;
                case 2 -> TRUE;  // Legacy ordinal for Nullablity.UNKNOWN
                default -> throw new IOException("Unknown ordinal [" + ordinal + "] for Nullability");
            };
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        if (usesBooleanFormat(out.getTransportVersion())) {
            out.writeBoolean(this == TRUE);
        } else {
            out.writeEnum(this);
        }
    }
}
