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

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Get snapshot request
 */
public class GetSnapshotsRequest extends MasterNodeRequest<GetSnapshotsRequest> {

    public static final String ALL_SNAPSHOTS = "_all";
    public static final String CURRENT_SNAPSHOT = "_current";
    public static final boolean DEFAULT_VERBOSE_MODE = true;
    public static final Version MULTIPLE_REPOSITORIES_SUPPORT_ADDED = Version.V_8_0_0;

    private String[] repositories;

    private String[] snapshots = Strings.EMPTY_ARRAY;

    private boolean ignoreUnavailable;

    private boolean verbose = DEFAULT_VERBOSE_MODE;

    public GetSnapshotsRequest() {
    }

    /**
     * Constructs a new get snapshots request with given repository names and list of snapshots
     *
     * @param repositories repository names
     * @param snapshots  list of snapshots
     */
    public GetSnapshotsRequest(String[] repositories, String[] snapshots) {
        this.repositories = repositories;
        this.snapshots = snapshots;
    }

    /**
     * Constructs a new get snapshots request with given repository names
     *
     * @param repositories repository names
     */
    public GetSnapshotsRequest(String... repositories) {
        this.repositories = repositories;
    }

    public GetSnapshotsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(MULTIPLE_REPOSITORIES_SUPPORT_ADDED)) {
            repositories = in.readStringArray();
        } else {
            repositories = new String[]{in.readString()};
        }
        snapshots = in.readStringArray();
        ignoreUnavailable = in.readBoolean();
        verbose = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(MULTIPLE_REPOSITORIES_SUPPORT_ADDED)) {
            out.writeStringArray(repositories);
        } else {
            if (repositories.length != 1) {
                throw new IllegalArgumentException("Requesting snapshots from multiple repositories is not supported in versions prior " +
                        "to " + MULTIPLE_REPOSITORIES_SUPPORT_ADDED.toString());
            }
            out.writeString(repositories[0]);
        }
        out.writeStringArray(snapshots);
        out.writeBoolean(ignoreUnavailable);
        out.writeBoolean(verbose);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repositories == null || repositories.length == 0) {
            validationException = addValidationError("repositories are missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets repository names
     *
     * @param repositories repository names
     * @return this request
     */
    public GetSnapshotsRequest repositories(String... repositories) {
        this.repositories = repositories;
        return this;
    }

    /**
     * Returns repository names
     *
     * @return repository names
     */
    public String[] repositories() {
        return this.repositories;
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
    public GetSnapshotsRequest snapshots(String[] snapshots) {
        this.snapshots = snapshots;
        return this;
    }

    /**
     * Set to true to ignore unavailable snapshots
     *
     * @return this request
     */
    public GetSnapshotsRequest ignoreUnavailable(boolean ignoreUnavailable) {
        this.ignoreUnavailable = ignoreUnavailable;
        return this;
    }

    /**
     * @return Whether snapshots should be ignored when unavailable (corrupt or temporarily not fetchable)
     */
    public boolean ignoreUnavailable() {
        return ignoreUnavailable;
    }

    /**
     * Set to {@code false} to only show the snapshot names and the indices they contain.
     * This is useful when the snapshots belong to a cloud-based repository where each
     * blob read is a concern (cost wise and performance wise), as the snapshot names and
     * indices they contain can be retrieved from a single index blob in the repository,
     * whereas the rest of the information requires reading a snapshot metadata file for
     * each snapshot requested.  Defaults to {@code true}, which returns all information
     * about each requested snapshot.
     */
    public GetSnapshotsRequest verbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    /**
     * Returns whether the request will return a verbose response.
     */
    public boolean verbose() {
        return verbose;
    }
}