/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Static utility class for working with {@link ConditionalClusterPrivilege} instances
 */
public final class ConditionalClusterPrivileges {

    public static final ConditionalClusterPrivilege[] EMPTY_ARRAY = new ConditionalClusterPrivilege[0];

    private ConditionalClusterPrivileges() {
    }

    /**
     * Utility method to read an array of {@link ConditionalClusterPrivilege} objects from a {@link StreamInput}
     */
    public static ConditionalClusterPrivilege[] readArray(StreamInput in) throws IOException {
        return in.readArray(in1 ->
            in1.readNamedWriteable(ConditionalClusterPrivilege.class), ConditionalClusterPrivilege[]::new);
    }

    /**
     * Utility method to write an array of {@link ConditionalClusterPrivilege} objects to a {@link StreamOutput}
     */
    public static void writeArray(StreamOutput out, ConditionalClusterPrivilege[] privileges) throws IOException {
        out.writeArray((out1, value) -> out1.writeNamedWriteable(value), privileges);
    }

}
