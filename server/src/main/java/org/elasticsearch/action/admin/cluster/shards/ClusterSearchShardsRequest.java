/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public final class ClusterSearchShardsRequest extends MasterNodeReadRequest<ClusterSearchShardsRequest>
    implements
        IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;
    @Nullable
    private String routing;
    @Nullable
    private String preference;
    private IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();

    public ClusterSearchShardsRequest(TimeValue masterNodeTimeout, String... indices) {
        super(masterNodeTimeout);
        indices(indices);
    }

    public ClusterSearchShardsRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    @Override
    public ClusterSearchShardsRequest indices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (int i = 0; i < indices.length; i++) {
            Objects.requireNonNull(indices[i], "indices[" + i + "] must not be null");
        }
        this.indices = indices;
        return this;
    }

    /**
     * The indices
     */
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public ClusterSearchShardsRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public ClusterSearchShardsRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public ClusterSearchShardsRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public ClusterSearchShardsRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }
}
