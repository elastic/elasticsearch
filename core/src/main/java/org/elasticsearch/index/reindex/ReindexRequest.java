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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.VersionType.INTERNAL;

/**
 * Request to reindex some documents from one index to another. This implements CompositeIndicesRequest but in a misleading way. Rather than
 * returning all the subrequests that it will make it tries to return a representative set of subrequests. This is best-effort for a bunch
 * of reasons, not least of which that scripts are allowed to change the destination request in drastic ways, including changing the index
 * to which documents are written.
 */
public class ReindexRequest extends AbstractBulkIndexByScrollRequest<ReindexRequest> implements CompositeIndicesRequest {
    /**
     * Prototype for index requests.
     */
    private IndexRequest destination;

    private RemoteInfo remoteInfo;

    public ReindexRequest() {
    }

    public ReindexRequest(SearchRequest search, IndexRequest destination) {
        this(search, destination, true);
    }

    private ReindexRequest(SearchRequest search, IndexRequest destination, boolean setDefaults) {
        super(search, setDefaults);
        this.destination = destination;
    }

    @Override
    protected ReindexRequest self() {
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = super.validate();
        if (getSearchRequest().indices() == null || getSearchRequest().indices().length == 0) {
            e = addValidationError("use _all if you really want to copy from all existing indexes", e);
        }
        if (getSearchRequest().source().fetchSource() != null && getSearchRequest().source().fetchSource().fetchSource() == false) {
            e = addValidationError("_source:false is not supported in this context", e);
        }
        /*
         * Note that we don't call index's validator - it won't work because
         * we'll be filling in portions of it as we receive the docs. But we can
         * validate some things so we do that below.
         */
        if (destination.index() == null) {
            e = addValidationError("index must be specified", e);
            return e;
        }
        if (false == routingIsValid()) {
            e = addValidationError("routing must be unset, [keep], [discard] or [=<some new value>]", e);
        }
        if (destination.versionType() == INTERNAL) {
            if (destination.version() != Versions.MATCH_ANY && destination.version() != Versions.MATCH_DELETED) {
                e = addValidationError("unsupported version for internal versioning [" + destination.version() + ']', e);
            }
        }
        if (getRemoteInfo() != null) {
            if (getSearchRequest().source().query() != null) {
                e = addValidationError("reindex from remote sources should use RemoteInfo's query instead of source's query", e);
            }
            if (getSlices() == AbstractBulkByScrollRequest.AUTO_SLICES || getSlices() > 1) {
                e = addValidationError("reindex from remote sources doesn't support slices > 1 but was [" + getSlices() + "]", e);
            }
        }
        return e;
    }

    private boolean routingIsValid() {
        if (destination.routing() == null || destination.routing().startsWith("=")) {
            return true;
        }
        switch (destination.routing()) {
        case "keep":
        case "discard":
            return true;
        default:
            return false;
        }
    }

    public IndexRequest getDestination() {
        return destination;
    }

    public void setRemoteInfo(RemoteInfo remoteInfo) {
        this.remoteInfo = remoteInfo;
    }

    public RemoteInfo getRemoteInfo() {
        return remoteInfo;
    }

    @Override
    public ReindexRequest forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices) {
        ReindexRequest sliced = doForSlice(new ReindexRequest(slice, destination, false), slicingTask, totalSlices);
        sliced.setRemoteInfo(remoteInfo);
        return sliced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        destination = new IndexRequest();
        destination.readFrom(in);
        remoteInfo = in.readOptionalWriteable(RemoteInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        destination.writeTo(out);
        out.writeOptionalWriteable(remoteInfo);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("reindex from ");
        if (remoteInfo != null) {
            b.append('[').append(remoteInfo).append(']');
        }
        searchToString(b);
        b.append(" to [").append(destination.index()).append(']');
        if (destination.type() != null) {
            b.append('[').append(destination.type()).append(']');
        }
        return b.toString();
    }
}
