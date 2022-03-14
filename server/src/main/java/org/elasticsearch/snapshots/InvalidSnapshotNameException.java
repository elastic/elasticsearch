/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown on the attempt to create a snapshot with invalid name
 */
public class InvalidSnapshotNameException extends SnapshotException {

    private final Reason reason;

    public InvalidSnapshotNameException(final String repositoryName, final String snapshotName, final Reason reason) {
        super(repositoryName, snapshotName, "Invalid snapshot name [" + snapshotName + "], " + reason.description);
        assert reason != Reason.MISSING : "Reason.MISSING is only for bwc purposes";
        this.reason = reason;
    }

    public InvalidSnapshotNameException(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_8_2_0)) {
            reason = in.readEnum(Reason.class);
        } else {
            reason = Reason.MISSING;
        }
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_2_0)) {
            out.writeEnum(reason);
        }
    }

    public Reason getReason() {
        return reason;
    }

    /**
     * The reason why the snapshot name was deemed invalid
     */
    public enum Reason {
        EMPTY("cannot be empty"),
        WHITESPACE("must not contain whitespace"),
        COMMA("must not contain ','"),
        HASHTAG("must not contain '#'"),
        STARTS_WITH_UNDERSCORE("must not start with '_'"),
        NOT_LOWERCASE("must be lowercase"),
        INVALID_FILENAME_CHARS("must not contain the following characters " + Strings.INVALID_FILENAME_CHARS),
        ALREADY_IN_PROGRESS("snapshot with the same name is already in-progress"),
        ALREADY_EXISTS("snapshot with the same name already exists"),
        MISSING("reason is missing in versions before 8.2.0, please check the error message");

        private final String description;

        Reason(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }
}
