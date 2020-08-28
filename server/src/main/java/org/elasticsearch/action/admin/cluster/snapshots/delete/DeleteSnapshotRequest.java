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
 * Delete snapshot request removes snapshots from the repository and cleans up all files that are associated with the snapshots.
 * All files that are shared with at least one other existing snapshot are left intact.
 */
public class DeleteSnapshotRequest extends MasterNodeRequest<DeleteSnapshotRequest> {

    private String repository;

    private String[] snapshots;

    /**
     * Constructs a new delete snapshots request
     */
    public DeleteSnapshotRequest() {
    }

    /**
     * Constructs a new delete snapshots request with repository and snapshot names
     *
     * @param repository repository name
     * @param snapshots  snapshot names
     */
    public DeleteSnapshotRequest(String repository, String... snapshots) {
        this.repository = repository;
        this.snapshots = snapshots;
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
        snapshots = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeStringArray(snapshots);
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
}
