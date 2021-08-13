/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Get snapshot status request
 */
public class SnapshotsStatusRequest extends MasterNodeRequest<SnapshotsStatusRequest> {

    private String repository = "_all";

    private String[] snapshots = Strings.EMPTY_ARRAY;

    private boolean ignoreUnavailable;

    public SnapshotsStatusRequest() {}

    /**
     * Constructs a new get snapshots request with given repository name and list of snapshots
     *
     * @param repository repository name
     * @param snapshots  list of snapshots
     */
    public SnapshotsStatusRequest(String repository, String[] snapshots) {
        this.repository = repository;
        this.snapshots = snapshots;
    }

    public SnapshotsStatusRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        snapshots = in.readStringArray();
        ignoreUnavailable = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeStringArray(snapshots);
        out.writeBoolean(ignoreUnavailable);
    }

    /**
     * Constructs a new get snapshots request with given repository name
     *
     * @param repository repository name
     */
    public SnapshotsStatusRequest(String repository) {
        this.repository = repository;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (snapshots == null) {
            validationException = addValidationError("snapshots is null", validationException);
        }
        return validationException;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    /**
     * Sets repository name
     *
     * @param repository repository name
     * @return this request
     */
    public SnapshotsStatusRequest repository(String repository) {
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
     * Returns the names of the snapshots.
     *
     * @return the names of snapshots
     */
    public String[] snapshots() {
        return this.snapshots;
    }

    /**
     * Sets the list of snapshots to be returned
     *
     * @return this request
     */
    public SnapshotsStatusRequest snapshots(String[] snapshots) {
        this.snapshots = snapshots;
        return this;
    }

    /**
     * Set to <code>true</code> to ignore unavailable snapshots, instead of throwing an exception.
     * Defaults to <code>false</code>, which means unavailable snapshots cause an exception to be thrown.
     *
     * @param ignoreUnavailable whether to ignore unavailable snapshots
     * @return this request
     */
    public SnapshotsStatusRequest ignoreUnavailable(boolean ignoreUnavailable) {
        this.ignoreUnavailable = ignoreUnavailable;
        return this;
    }

    /**
     * Returns whether the request permits unavailable snapshots to be ignored.
     *
     * @return true if the request will ignore unavailable snapshots, false if it will throw an exception on unavailable snapshots
     */
    public boolean ignoreUnavailable() {
        return ignoreUnavailable;
    }
}
