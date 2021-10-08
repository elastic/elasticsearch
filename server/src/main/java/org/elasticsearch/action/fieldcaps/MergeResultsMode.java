/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;


/**
 * Used in {@link FieldCapabilitiesNodeRequest} and {@link FieldCapabilitiesRequest} to
 * instruct a remote node or remote cluster on how to combine field caps index responses.
 *
 * @see FieldCapabilitiesNodeRequest#getMergeMode()
 * @see FieldCapabilitiesRequest#getMergeMode()
 */
enum MergeResultsMode implements Writeable {

    /**
     * The default mode of {@link FieldCapabilitiesRequest}. This mode fully combines results for end users.
     */
    FULL_MERGE,

    /**
     * Used internally in {@link FieldCapabilitiesNodeRequest} or {@link FieldCapabilitiesNodeRequest} when the
     * cluster is upgraded. This mode combines multiple responses in a compact form which can be combined again
     * with other responses before generating a fully combined result for end users.
     */
    INTERNAL_PARTIAL_MERGE,

    /**
     * Used internally in {@link FieldCapabilitiesNodeRequest} or {@link FieldCapabilitiesNodeRequest} in mixed
     * clusters when the coordinating cluster is still on the old version.
     */
    NO_MERGE;

    static MergeResultsMode readValue(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_16_0)) {
            return in.readEnum(MergeResultsMode.class);
        } else {
            return in.readBoolean() ? FULL_MERGE : NO_MERGE;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_16_0)) {
            out.writeEnum(this);
        } else {
            out.writeBoolean(this == FULL_MERGE);
        }
    }
}
