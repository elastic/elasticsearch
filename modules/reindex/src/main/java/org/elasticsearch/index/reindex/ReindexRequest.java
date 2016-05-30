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
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;
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

    public ReindexRequest() {
    }

    public ReindexRequest(SearchRequest search, IndexRequest destination) {
        super(search);
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
        if (destination.ttl() != null) {
            e = addValidationError("setting ttl on destination isn't supported. use scripts instead.", e);
        }
        if (destination.timestamp() != null) {
            e = addValidationError("setting timestamp on destination isn't supported. use scripts instead.", e);
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        destination = new IndexRequest();
        destination.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        destination.writeTo(out);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("reindex from ");
        searchToString(b);
        b.append(" to [").append(destination.index()).append(']');
        if (destination.type() != null) {
            b.append('[').append(destination.type()).append(']');
        }
        return b.toString();
    }

    // CompositeIndicesRequest implementation so plugins can reason about the request. This is really just a best effort thing.
    /**
     * Accessor to get the underlying {@link IndicesRequest}s that this request wraps. Note that this method is <strong>not
     * accurate</strong> since it returns a prototype {@link IndexRequest} and not the actual requests that will be issued as part of the
     * execution of this request. Additionally, scripts can modify the underlying {@link IndexRequest} and change values such as the index,
     * type, {@link org.elasticsearch.action.support.IndicesOptions}. In short - only use this for very course reasoning about the request.
     *
     * @return a list comprising of the {@link SearchRequest} and the prototype {@link IndexRequest}
     */
    @Override
    public List<? extends IndicesRequest> subRequests() {
        assert getSearchRequest() != null;
        assert getDestination() != null;
        return unmodifiableList(Arrays.asList(getSearchRequest(), getDestination()));
    }
}
