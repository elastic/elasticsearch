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

import org.elasticsearch.Version;
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
public class DeleteSnapshotsRequest extends MasterNodeRequest<DeleteSnapshotsRequest> {

    public static final Version MULTI_DELETE_VERSION = Version.V_8_0_0;

    private String repository;

    private String[] snapshots;

    /**
     * Constructs a new delete snapshots request
     */
    public DeleteSnapshotsRequest() {
    }

    /**
     * Constructs a new delete snapshots request with repository and snapshot name
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public DeleteSnapshotsRequest(String repository, String snapshot) {
        this(repository, new String[]{snapshot});
    }

        /**
         * Constructs a new delete snapshots request with repository and snapshot name
         *
         * @param repository repository name
         * @param snapshots  snapshot names
         */
    public DeleteSnapshotsRequest(String repository, String[] snapshots) {
        this.repository = repository;
        this.snapshots = snapshots;
    }

    /**
     * Constructs a new delete snapshots request with repository name
     *
     * @param repository repository name
     */
    public DeleteSnapshotsRequest(String repository) {
        this.repository = repository;
    }

    public DeleteSnapshotsRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        if (in.getVersion().onOrAfter(MULTI_DELETE_VERSION)) {
            snapshots = in.readStringArray();
        } else {
            snapshots = new String[] {in.readString()};
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        if (out.getVersion().onOrAfter(MULTI_DELETE_VERSION)) {
            out.writeStringArray(snapshots);
        } else {
            if (snapshots.length != 1) {
                throw new IllegalArgumentException(
                    "Can't write snapshot delete with more than one snapshot to version [" + out.getVersion() + "]");
            }
            out.writeString(snapshots[0]);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (snapshots == null || snapshots.length == 0) {
            validationException = addValidationError("snapshot is missing", validationException);
        }
        return validationException;
    }


    public DeleteSnapshotsRequest repository(String repository) {
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
    public String[] snapshots() {
        return this.snapshots;
    }

    /**
     * Sets snapshot name
     *
     * @return this request
     */
    public DeleteSnapshotsRequest snapshots(String[] snapshots) {
        this.snapshots = snapshots;
        return this;
    }
}
