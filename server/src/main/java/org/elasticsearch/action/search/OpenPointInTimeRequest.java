/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class OpenPointInTimeRequest extends UntypedActionRequest implements IndicesRequest.Replaceable {

    private String[] indices;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
    private TimeValue keepAlive;
    private int maxConcurrentShardRequests = SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS;
    @Nullable
    private String routing;
    private boolean routingFromSlice;
    @Nullable
    private String searchSlice;
    @Nullable
    private String preference;

    private ResolvedIndexExpressions resolvedIndexExpressions;
    @Nullable
    private transient TargetProjects resolvedTargetProjects;
    @Nullable
    private String projectRouting;

    private QueryBuilder indexFilter;

    private boolean allowPartialSearchResults = false;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = SearchRequest.DEFAULT_INDICES_OPTIONS;
    public static final IndicesOptions DEFAULT_CPS_INDICES_OPTIONS = SearchRequest.DEFAULT_CPS_INDICES_OPTIONS;

    public OpenPointInTimeRequest(String... indices) {
        this.indices = Objects.requireNonNull(indices, "[index] is not specified");
    }

    public OpenPointInTimeRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.keepAlive = in.readTimeValue();
        this.routing = in.readOptionalString();
        this.preference = in.readOptionalString();
        this.maxConcurrentShardRequests = in.readVInt();
        this.indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        this.allowPartialSearchResults = in.readBoolean();
        if (in.getTransportVersion().supports(SliceIndexing.OPEN_POINT_IN_TIME_SLICE_ROUTING_STATE_VERSION)) {
            this.routingFromSlice = in.readBoolean();
            this.searchSlice = in.readOptionalString();
        } else {
            this.routingFromSlice = false;
            this.searchSlice = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeTimeValue(keepAlive);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeVInt(maxConcurrentShardRequests);
        out.writeOptionalWriteable(indexFilter);
        out.writeBoolean(allowPartialSearchResults);
        if (out.getTransportVersion().supports(SliceIndexing.OPEN_POINT_IN_TIME_SLICE_ROUTING_STATE_VERSION)) {
            out.writeBoolean(routingFromSlice);
            out.writeOptionalString(searchSlice);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null || indices.length == 0) {
            validationException = addValidationError("[index] is not specified", validationException);
        }
        if (keepAlive == null) {
            validationException = addValidationError("[keep_alive] is not specified", validationException);
        }
        if (projectRouting != null && indicesOptions.resolveCrossProjectIndexExpression() == false) {
            validationException = ValidateActions.addValidationError(
                "Unknown key for a VALUE_STRING in [project_routing]",
                validationException
            );

        }
        if (routingFromSlice && SliceIndexing.SLICE_FEATURE_FLAG.isEnabled() == false) {
            validationException = addValidationError("request does not support [slice]", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public OpenPointInTimeRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public OpenPointInTimeRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "[indices_options] parameter must be non null");
        return this;
    }

    public TimeValue keepAlive() {
        return keepAlive;
    }

    /**
     * Set keep alive for the point in time
     */
    public OpenPointInTimeRequest keepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public String routing() {
        return routing;
    }

    public OpenPointInTimeRequest routing(String routing) {
        if (routing != null && routingFromSlice) {
            throw new IllegalArgumentException("[routing] is not allowed together with [slice]");
        }
        this.routing = routing;
        return this;
    }

    /**
     * Returns {@code true} when routing was provided through the {@code slice} REST parameter.
     */
    public boolean isRoutingFromSlice() {
        return routingFromSlice;
    }

    /**
     * Returns the requested {@code slice} value when routing comes from {@code slice}.
     */
    @Nullable
    public String searchSlice() {
        return searchSlice;
    }

    /**
     * Sets the user-provided {@code slice} value and derives routing/provenance from it.
     * Passing {@code null} clears slice-routing provenance and any routing previously derived from {@code slice}.
     */
    public OpenPointInTimeRequest searchSlice(@Nullable String searchSlice) {
        if (searchSlice != null) {
            if (SliceIndexing.SLICE_FEATURE_FLAG.isEnabled() == false) {
                throw new IllegalArgumentException("request does not support [slice]");
            }
            if (routing != null && routingFromSlice == false) {
                throw new IllegalArgumentException("[routing] is not allowed together with [slice]");
            }
        }
        this.searchSlice = searchSlice;
        if (searchSlice == null) {
            if (routingFromSlice) {
                this.routing = null;
            }
            this.routingFromSlice = false;
        } else {
            this.routingFromSlice = true;
            this.routing = SliceIndexing.SLICE_ALL.equals(searchSlice) ? null : searchSlice;
        }
        return this;
    }

    public String preference() {
        return preference;
    }

    public OpenPointInTimeRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    /**
     * Similar to {@link SearchRequest#getMaxConcurrentShardRequests()}, this returns the number of shard requests that should be
     * executed concurrently on a single node . This value should be used as a protection mechanism to reduce the number of shard
     * requests fired per open point-in-time request. The default is {@code 5}
     */
    public int maxConcurrentShardRequests() {
        return maxConcurrentShardRequests;
    }

    /**
     * Similar to {@link SearchRequest#setMaxConcurrentShardRequests(int)}, this sets the number of shard requests that should be
     * executed concurrently on a single node. This value should be used as a protection mechanism to reduce the number of shard
     * requests fired per open point-in-time request.
     */
    public void maxConcurrentShardRequests(int maxConcurrentShardRequests) {
        if (maxConcurrentShardRequests < 1) {
            throw new IllegalArgumentException("maxConcurrentShardRequests must be >= 1");
        }
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
    }

    public void indexFilter(QueryBuilder indexFilter) {
        this.indexFilter = indexFilter;
    }

    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    @Override
    public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
        this.resolvedIndexExpressions = expressions;
    }

    @Override
    public ResolvedIndexExpressions getResolvedIndexExpressions() {
        return resolvedIndexExpressions;
    }

    @Override
    public void setResolvedTargetProjects(TargetProjects targetProjects) {
        this.resolvedTargetProjects = targetProjects;
    }

    @Override
    @Nullable
    public TargetProjects getResolvedTargetProjects() {
        return resolvedTargetProjects;
    }

    @Override
    public boolean allowsCrossProject() {
        return true;
    }

    @Override
    public String getProjectRouting() {
        return projectRouting;
    }

    public void projectRouting(@Nullable String projectRouting) {
        if (this.projectRouting != null) {
            throw new IllegalArgumentException("project_routing is already set to [" + this.projectRouting + "]");
        }
        this.projectRouting = projectRouting;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public boolean allowPartialSearchResults() {
        return allowPartialSearchResults;
    }

    public OpenPointInTimeRequest allowPartialSearchResults(boolean allowPartialSearchResults) {
        this.allowPartialSearchResults = allowPartialSearchResults;
        return this;
    }

    @Override
    public String getDescription() {
        return "open search context: indices [" + String.join(",", indices) + "] keep_alive [" + keepAlive + "]";
    }

    @Override
    public String toString() {
        return "OpenPointInTimeRequest{"
            + "indices="
            + Arrays.toString(indices)
            + ", keepAlive="
            + keepAlive
            + ", maxConcurrentShardRequests="
            + maxConcurrentShardRequests
            + ", routing='"
            + routing
            + '\''
            + ", searchSlice='"
            + searchSlice
            + '\''
            + ", preference='"
            + preference
            + '\''
            + ", allowPartialSearchResults="
            + allowPartialSearchResults
            + '}';
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, this::getDescription, parentTaskId, headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenPointInTimeRequest that = (OpenPointInTimeRequest) o;
        return maxConcurrentShardRequests == that.maxConcurrentShardRequests
            && Arrays.equals(indices, that.indices)
            && indicesOptions.equals(that.indicesOptions)
            && keepAlive.equals(that.keepAlive)
            && Objects.equals(routing, that.routing)
            && routingFromSlice == that.routingFromSlice
            && Objects.equals(searchSlice, that.searchSlice)
            && Objects.equals(preference, that.preference)
            && Objects.equals(allowPartialSearchResults, that.allowPartialSearchResults);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            indicesOptions,
            keepAlive,
            maxConcurrentShardRequests,
            routing,
            routingFromSlice,
            searchSlice,
            preference,
            allowPartialSearchResults
        );
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }
}
