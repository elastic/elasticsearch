/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.globalstate;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Get snapshot global state status request
 */
public class SnapshotGlobalStateRequest extends MasterNodeRequest<SnapshotGlobalStateRequest> {

    private final String repository;

    private final String snapshot;

    /**
     * Constructs a new get snapshots request with given repository name and list of snapshots
     *
     * @param repository repository name
     * @param snapshot  snapshot name
     */
    public SnapshotGlobalStateRequest(String repository, String snapshot) {
        this.repository = Objects.requireNonNull(repository);
        this.snapshot = Objects.requireNonNull(snapshot);
    }

    public SnapshotGlobalStateRequest(StreamInput in) throws IOException {
        super(in);
        assert in.getTransportVersion().onOrAfter(TransportVersions.SNAPSHOT_GLOBAL_STATE_API_ADDED) : in.getTransportVersion();
        repository = in.readString();
        snapshot = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        assert out.getTransportVersion().onOrAfter(TransportVersions.SNAPSHOT_GLOBAL_STATE_API_ADDED) : out.getTransportVersion();
        out.writeString(repository);
        out.writeString(snapshot);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
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
    public String snapshot() {
        return this.snapshot;
    }

    @Override
    public String getDescription() {
        return "repository[" + repository + "], snapshot[" + snapshot + "]";
    }

}
