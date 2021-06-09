/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Get snapshot request
 */
public class GetSnapshotsRequest extends MasterNodeRequest<GetSnapshotsRequest> {

    public static final String ALL_SNAPSHOTS = "_all";
    public static final String CURRENT_SNAPSHOT = "_current";
    public static final boolean DEFAULT_VERBOSE_MODE = true;

    public static final Version MULTIPLE_REPOSITORIES_SUPPORT_ADDED = Version.V_8_0_0;

    public static final Version PAGINATED_GET_SNAPSHOTS_VERSION = Version.V_8_0_0;

    private int size = 0;

    @Nullable
    private After after;

    private GetSnapshotsAction.SortBy sort = GetSnapshotsAction.SortBy.START_TIME;

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
        if (in.getVersion().onOrAfter(PAGINATED_GET_SNAPSHOTS_VERSION)) {
            after = in.readOptionalWriteable(After::new);
            sort = in.readEnum(GetSnapshotsAction.SortBy.class);
            size = in.readVInt();
        }
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
        if (out.getVersion().onOrAfter(PAGINATED_GET_SNAPSHOTS_VERSION)) {
            out.writeOptionalWriteable(after);
            out.writeEnum(sort);
            out.writeVInt(size);
        }
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

    public GetSnapshotsRequest pagination(After after, GetSnapshotsAction.SortBy sort, int size) {
        this.after = after;
        this.sort = sort;
        this.size = size;
        return this;
    }

    public After after() {
        return after;
    }

    public GetSnapshotsAction.SortBy sort() {
        return sort;
    }

    public int size() {
        return size;
    }

    /**
     * Returns whether the request will return a verbose response.
     */
    public boolean verbose() {
        return verbose;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    public static final class After implements Writeable {

        private final String value;

        private final String snapshotName;

        After(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
        }

        @Nullable
        public static After from(@Nullable SnapshotInfo snapshotInfo, GetSnapshotsAction.SortBy sortBy) {
            if (snapshotInfo == null) {
                return null;
            }
            final String afterValue;
            switch (sortBy) {
                case START_TIME:
                    afterValue = String.valueOf(snapshotInfo.startTime());
                    break;
                case NAME:
                    afterValue = snapshotInfo.snapshotId().getName();
                    break;
                case DURATION:
                    afterValue = String.valueOf(snapshotInfo.endTime() - snapshotInfo.startTime());
                    break;
                case INDICES:
                    afterValue = String.valueOf(snapshotInfo.indices().size());
                    break;
                default:
                    throw new AssertionError("unknown sort column [" + sortBy + "]");
            }
            return new After(afterValue, snapshotInfo.snapshotId().getName());
        }

        public After(String value, String snapshotName) {
            this.value = value;
            this.snapshotName = snapshotName;
        }

        public String value() {
            return value;
        }

        public String snapshotName() {
            return snapshotName;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
            out.writeString(snapshotName);
        }
    }
}
