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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Get snapshot request
 */
public class GetSnapshotsRequest extends MasterNodeRequest<GetSnapshotsRequest> {

    public static final String ALL_SNAPSHOTS = "_all";
    public static final String CURRENT_SNAPSHOT = "_current";
    public static final boolean DEFAULT_VERBOSE_MODE = true;

    public static final Version MULTIPLE_REPOSITORIES_SUPPORT_ADDED = Version.V_7_14_0;

    public static final Version PAGINATED_GET_SNAPSHOTS_VERSION = Version.V_7_14_0;

    public static final Version NUMERIC_PAGINATION_VERSION = Version.V_7_15_0;

    public static final int NO_LIMIT = -1;

    /**
     * Number of snapshots to fetch information for or {@link #NO_LIMIT} for fetching all snapshots matching the request.
     */
    private int size = NO_LIMIT;

    /**
     * Numeric offset at which to start fetching snapshots. Mutually exclusive with {@link After} if not equal to {@code 0}.
     */
    private int offset = 0;

    @Nullable
    private After after;

    private SortBy sort = SortBy.START_TIME;

    private SortOrder order = SortOrder.ASC;

    private String[] repositories;

    private String[] snapshots = Strings.EMPTY_ARRAY;

    private boolean ignoreUnavailable;

    private boolean verbose = DEFAULT_VERBOSE_MODE;

    public GetSnapshotsRequest() {}

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
            repositories = new String[] { in.readString() };
        }
        snapshots = in.readStringArray();
        ignoreUnavailable = in.readBoolean();
        verbose = in.readBoolean();
        if (in.getVersion().onOrAfter(PAGINATED_GET_SNAPSHOTS_VERSION)) {
            after = in.readOptionalWriteable(After::new);
            sort = in.readEnum(SortBy.class);
            size = in.readVInt();
            order = SortOrder.readFromStream(in);
            if (in.getVersion().onOrAfter(NUMERIC_PAGINATION_VERSION)) {
                offset = in.readVInt();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(MULTIPLE_REPOSITORIES_SUPPORT_ADDED)) {
            out.writeStringArray(repositories);
        } else {
            if (repositories.length != 1) {
                throw new IllegalArgumentException(
                    "Requesting snapshots from multiple repositories is not supported in versions prior "
                        + "to "
                        + MULTIPLE_REPOSITORIES_SUPPORT_ADDED.toString()
                );
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
            order.writeTo(out);
            if (out.getVersion().onOrAfter(NUMERIC_PAGINATION_VERSION)) {
                out.writeVInt(offset);
            } else if (offset != 0) {
                throw new IllegalArgumentException(
                    "can't use numeric offset in get snapshots request with node version [" + out.getVersion() + "]"
                );
            }
        } else if (sort != SortBy.START_TIME || size != NO_LIMIT || after != null || order != SortOrder.ASC) {
            throw new IllegalArgumentException("can't use paginated get snapshots request with node version [" + out.getVersion() + "]");
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repositories == null || repositories.length == 0) {
            validationException = addValidationError("repositories are missing", validationException);
        }
        if (size == 0 || size < NO_LIMIT) {
            validationException = addValidationError("size must be -1 or greater than 0", validationException);
        }
        if (verbose == false) {
            if (sort != SortBy.START_TIME) {
                validationException = addValidationError("can't use non-default sort with verbose=false", validationException);
            }
            if (size > 0) {
                validationException = addValidationError("can't use size limit with verbose=false", validationException);
            }
            if (offset > 0) {
                validationException = addValidationError("can't use offset with verbose=false", validationException);
            }
            if (after != null) {
                validationException = addValidationError("can't use after with verbose=false", validationException);
            }
            if (order != SortOrder.ASC) {
                validationException = addValidationError("can't use non-default sort order with verbose=false", validationException);
            }
        } else if (after != null && offset > 0) {
            validationException = addValidationError("can't use after and offset simultaneously", validationException);
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

    public boolean isSingleRepositoryRequest() {
        return repositories.length == 1
            && repositories[0] != null
            && "_all".equals(repositories[0]) == false
            && Regex.isSimpleMatchPattern(repositories[0]) == false;
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

    public After after() {
        return after;
    }

    public SortBy sort() {
        return sort;
    }

    public GetSnapshotsRequest after(@Nullable After after) {
        this.after = after;
        return this;
    }

    public GetSnapshotsRequest sort(SortBy sort) {
        this.sort = sort;
        return this;
    }

    public GetSnapshotsRequest size(int size) {
        this.size = size;
        return this;
    }

    public int size() {
        return size;
    }

    public int offset() {
        return offset;
    }

    public GetSnapshotsRequest offset(int offset) {
        this.offset = offset;
        return this;
    }

    public SortOrder order() {
        return order;
    }

    public GetSnapshotsRequest order(SortOrder order) {
        this.order = order;
        return this;
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

    public enum SortBy {
        START_TIME("start_time"),
        NAME("name"),
        DURATION("duration"),
        INDICES("index_count");

        private final String param;

        SortBy(String param) {
            this.param = param;
        }

        @Override
        public String toString() {
            return param;
        }

        public static SortBy of(String value) {
            switch (value) {
                case "start_time":
                    return START_TIME;
                case "name":
                    return NAME;
                case "duration":
                    return DURATION;
                case "index_count":
                    return INDICES;
                default:
                    throw new IllegalArgumentException("unknown sort order [" + value + "]");
            }
        }
    }

    public static final class After implements Writeable {

        private final String value;

        private final String repoName;

        private final String snapshotName;

        After(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), in.readString());
        }

        public static After fromQueryParam(String param) {
            final String[] parts = new String(Base64.getUrlDecoder().decode(param), StandardCharsets.UTF_8).split(",");
            if (parts.length != 3) {
                throw new IllegalArgumentException("invalid ?after parameter [" + param + "]");
            }
            return new After(parts[0], parts[1], parts[2]);
        }

        @Nullable
        public static After from(@Nullable SnapshotInfo snapshotInfo, SortBy sortBy) {
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
            return new After(afterValue, snapshotInfo.repository(), snapshotInfo.snapshotId().getName());
        }

        public After(String value, String repoName, String snapshotName) {
            this.value = value;
            this.repoName = repoName;
            this.snapshotName = snapshotName;
        }

        public String value() {
            return value;
        }

        public String snapshotName() {
            return snapshotName;
        }

        public String repoName() {
            return repoName;
        }

        public String asQueryParam() {
            return Base64.getUrlEncoder().encodeToString((value + "," + repoName + "," + snapshotName).getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
            out.writeString(repoName);
            out.writeString(snapshotName);
        }
    }
}
