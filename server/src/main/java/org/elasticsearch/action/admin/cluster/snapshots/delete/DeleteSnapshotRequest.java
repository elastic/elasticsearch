/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.snapshots.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Delete snapshot request
 * <p>
 * Delete snapshot request removes the snapshot record from the repository and cleans up all
 * files that are associated with this particular snapshot. All files that are shared with
 * at least one other existing snapshot are left intact.
 */
public class DeleteSnapshotRequest extends MasterNodeRequest<DeleteSnapshotRequest> {

    private String repository;

    private String snapshot;

    /**
     * Constructs a new delete snapshots request
     */
    public DeleteSnapshotRequest() {
    }

    /**
     * Constructs a new delete snapshots request with repository and snapshot name
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public DeleteSnapshotRequest(String repository, String snapshot) {
        this.repository = repository;
        this.snapshot = snapshot;
    }

    /**
     * Constructs a new delete snapshots request with repository name
     *
     * @param repository repository name
     */
    public DeleteSnapshotRequest(String repository) {
        this.repository = repository;
    }

    public DeleteSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        snapshot = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeString(snapshot);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (snapshot == null) {
            validationException = addValidationError("snapshot is missing", validationException);
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
     * Returns repository name
     *
     * @return repository name
     */
    public String snapshot() {
        return this.snapshot;
    }

    /**
     * Sets snapshot name
     *
     * @return this request
     */
    public DeleteSnapshotRequest snapshot(String snapshot) {
        this.snapshot = snapshot;
        return this;
    }
}
