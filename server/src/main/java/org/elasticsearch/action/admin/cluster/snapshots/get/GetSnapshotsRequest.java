/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Get snapshot request
 */
public class GetSnapshotsRequest extends MasterNodeRequest<GetSnapshotsRequest> {

    public static final String CURRENT_SNAPSHOT = "_current";
    public static final String NO_POLICY_PATTERN = "_none";
    public static final boolean DEFAULT_VERBOSE_MODE = true;

    private static final TransportVersion INDICES_FLAG_VERSION = TransportVersions.V_8_3_0;
    private static final TransportVersion STATE_FLAG_VERSION = TransportVersions.STATE_PARAM_GET_SNAPSHOT;

    public static final int NO_LIMIT = -1;

    /**
     * Number of snapshots to fetch information for or {@link #NO_LIMIT} for fetching all snapshots matching the request.
     */
    private int size = NO_LIMIT;

    /**
     * Numeric offset at which to start fetching snapshots. Mutually exclusive with {@link #after} if not equal to {@code 0}.
     */
    private int offset = 0;

    /**
     * Sort key value at which to start fetching snapshots. Mutually exclusive with {@link #offset} if not {@code null}.
     */
    @Nullable
    private SnapshotSortKey.After after;

    @Nullable
    private String fromSortValue;

    private SnapshotSortKey sort = SnapshotSortKey.START_TIME;

    private SortOrder order = SortOrder.ASC;

    private String[] repositories;

    private String[] snapshots = Strings.EMPTY_ARRAY;

    private String[] policies = Strings.EMPTY_ARRAY;

    private boolean ignoreUnavailable;

    private boolean verbose = DEFAULT_VERBOSE_MODE;

    private boolean includeIndexNames = true;

    private EnumSet<SnapshotState> state = EnumSet.noneOf(SnapshotState.class);

    public GetSnapshotsRequest(TimeValue masterNodeTimeout) {
        super(masterNodeTimeout);
    }

    /**
     * Constructs a new get snapshots request with given repository names and list of snapshots
     *
     * @param repositories repository names
     * @param snapshots  list of snapshots
     */
    public GetSnapshotsRequest(TimeValue masterNodeTimeout, String[] repositories, String[] snapshots) {
        this(masterNodeTimeout, repositories);
        this.snapshots = snapshots;
    }

    /**
     * Constructs a new get snapshots request with given repository names
     *
     * @param repositories repository names
     */
    public GetSnapshotsRequest(TimeValue masterNodeTimeout, String... repositories) {
        this(masterNodeTimeout);
        this.repositories = repositories;
    }

    public GetSnapshotsRequest(StreamInput in) throws IOException {
        super(in);
        repositories = in.readStringArray();
        snapshots = in.readStringArray();
        ignoreUnavailable = in.readBoolean();
        verbose = in.readBoolean();
        after = in.readOptionalWriteable(SnapshotSortKey.After::new);
        sort = in.readEnum(SnapshotSortKey.class);
        size = in.readVInt();
        order = SortOrder.readFromStream(in);
        offset = in.readVInt();
        policies = in.readStringArray();
        fromSortValue = in.readOptionalString();
        if (in.getTransportVersion().onOrAfter(INDICES_FLAG_VERSION)) {
            includeIndexNames = in.readBoolean();
        }
        if (in.getTransportVersion().onOrAfter(STATE_FLAG_VERSION)) {
            state = in.readEnumSet(SnapshotState.class);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(repositories);
        out.writeStringArray(snapshots);
        out.writeBoolean(ignoreUnavailable);
        out.writeBoolean(verbose);
        out.writeOptionalWriteable(after);
        out.writeEnum(sort);
        out.writeVInt(size);
        order.writeTo(out);
        out.writeVInt(offset);
        out.writeStringArray(policies);
        out.writeOptionalString(fromSortValue);
        if (out.getTransportVersion().onOrAfter(INDICES_FLAG_VERSION)) {
            out.writeBoolean(includeIndexNames);
        }
        if (out.getTransportVersion().onOrAfter(STATE_FLAG_VERSION)) {
            out.writeEnumSet(state);
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
            if (sort != SnapshotSortKey.START_TIME) {
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
            if (policies.length != 0) {
                validationException = addValidationError("can't use slm policy filter with verbose=false", validationException);
            }
            if (fromSortValue != null) {
                validationException = addValidationError("can't use from_sort_value with verbose=false", validationException);
            }
        } else if (offset > 0) {
            if (after != null) {
                validationException = addValidationError("can't use after and offset simultaneously", validationException);
            }
        } else if (after != null && fromSortValue != null) {
            validationException = addValidationError("can't use after and from_sort_value simultaneously", validationException);
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
     * Sets slm policy patterns
     *
     * @param policies policy patterns
     * @return this request
     */
    public GetSnapshotsRequest policies(String... policies) {
        this.policies = policies;
        return this;
    }

    /**
     * Returns policy patterns
     *
     * @return policy patterns
     */
    public String[] policies() {
        return policies;
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

    public GetSnapshotsRequest includeIndexNames(boolean indices) {
        this.includeIndexNames = indices;
        return this;
    }

    public boolean includeIndexNames() {
        return includeIndexNames;
    }

    @Nullable
    public SnapshotSortKey.After after() {
        return after;
    }

    public SnapshotSortKey sort() {
        return sort;
    }

    public GetSnapshotsRequest after(@Nullable SnapshotSortKey.After after) {
        this.after = after;
        return this;
    }

    public GetSnapshotsRequest fromSortValue(@Nullable String fromSortValue) {
        this.fromSortValue = fromSortValue;
        return this;
    }

    @Nullable
    public String fromSortValue() {
        return fromSortValue;
    }

    public GetSnapshotsRequest sort(SnapshotSortKey sort) {
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

    public EnumSet<SnapshotState> state() {
        return state;
    }

    public GetSnapshotsRequest state(EnumSet<SnapshotState> state) {
        this.state = state;
        return this;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        final StringBuilder stringBuilder = new StringBuilder("repositories[");
        Strings.collectionToDelimitedStringWithLimit(Arrays.asList(repositories), ",", 512, stringBuilder);
        stringBuilder.append("], snapshots[");
        Strings.collectionToDelimitedStringWithLimit(Arrays.asList(snapshots), ",", 1024, stringBuilder);
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}
