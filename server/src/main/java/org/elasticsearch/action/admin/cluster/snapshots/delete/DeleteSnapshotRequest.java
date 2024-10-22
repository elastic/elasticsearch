/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.delete;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Delete snapshot request
 * <p>
 * Delete snapshot request removes snapshots from the repository and cleans up all files that are associated with the snapshots.
 * All files that are shared with at least one other existing snapshot are left intact.
 */
public class DeleteSnapshotRequest extends MasterNodeRequest<DeleteSnapshotRequest> {

    private String repository;

    private String[] snapshots;
    private boolean waitForCompletion = true;

    /**
     * Constructs a new delete snapshots request with repository and snapshot names
     *
     * @param repository repository name
     * @param snapshots  snapshot names
     */
    public DeleteSnapshotRequest(TimeValue masterNodeTimeout, String repository, String... snapshots) {
        super(masterNodeTimeout);
        this.repository = repository;
        this.snapshots = snapshots;
    }

    public DeleteSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        snapshots = in.readStringArray();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            waitForCompletion = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeStringArray(snapshots);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeBoolean(waitForCompletion);
        } else {
            assert waitForCompletion : "Using wait_for_completion parameter when it should have been disallowed";
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (snapshots == null || snapshots.length == 0) {
            validationException = addValidationError("snapshots are missing", validationException);
        }
        return validationException;
    }

    public DeleteSnapshotRequest repository(String repository) {
        this.repository = repository;
        return this;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String repository() {
        return this.repository;
    }

    /**
     * Returns snapshot names
     *
     * @return snapshot names
     */
    public String[] snapshots() {
        return this.snapshots;
    }

    /**
     * Sets snapshot names
     *
     * @return this request
     */
    public DeleteSnapshotRequest snapshots(String... snapshots) {
        this.snapshots = snapshots;
        return this;
    }

    @Override
    public String getDescription() {
        return Strings.format("[%s]%s", repository, Arrays.toString(snapshots));
    }

    /**
     * If set to false the operation should return without waiting for the deletion to complete.
     *
     * By default, the operation will wait until all matching snapshots are deleted. It can be changed by setting this
     * flag to false.
     *
     * @param waitForCompletion true if operation should wait for the snapshot deletion
     * @return this request
     */
    public DeleteSnapshotRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns true if the request should wait for the snapshot delete(s) to complete before returning
     *
     * @return true if the request should wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

}
