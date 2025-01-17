/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.master.info;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public abstract class ClusterInfoRequest<Request extends ClusterInfoRequest<Request>> extends LocalClusterStateRequest
    implements
        IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;

    private IndicesOptions indicesOptions;

    protected ClusterInfoRequest(TimeValue masterTimeout, IndicesOptions indicesOptions) {
        super(masterTimeout);
        this.indicesOptions = indicesOptions;
    }

    /**
     * Even though most inheritors of {@link LocalClusterStateRequest} only allow reading from the transport wire for BwC reasons,
     * this constructor will need to stay after v10 as well as {@link org.elasticsearch.action.admin.indices.get.GetIndexRequest} is a
     * cross-cluster API and will thus always need to support serializing requests from and to the transport wire.
     */
    protected ClusterInfoRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            in.readStringArray();
        }
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Request indices(String... indices) {
        this.indices = indices;
        return (Request) this;
    }

    @SuppressWarnings("unchecked")
    public Request indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return (Request) this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }
}
