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

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

/**
 * Request to update some documents. That means you can't change their type, id, index, or anything like that. This implements
 * CompositeIndicesRequest but in a misleading way. Rather than returning all the subrequests that it will make it tries to return a
 * representative set of subrequests. This is best-effort but better than {@linkplain ReindexRequest} because scripts can't change the
 * destination index and things.
 */
public class UpdateByQueryRequest extends AbstractBulkIndexByScrollRequest<UpdateByQueryRequest> implements IndicesRequest.Replaceable {
    /**
     * Ingest pipeline to set on index requests made by this action.
     */
    private String pipeline;

    public UpdateByQueryRequest() {
    }

    public UpdateByQueryRequest(SearchRequest search) {
        this(search, true);
    }

    private UpdateByQueryRequest(SearchRequest search, boolean setDefaults) {
        super(search, setDefaults);
    }

    /**
     * Set the ingest pipeline to set on index requests made by this action.
     */
    public void setPipeline(String pipeline) {
        this.pipeline = pipeline;
    }

    /**
     * Ingest pipeline to set on index requests made by this action.
     */
    public String getPipeline() {
        return pipeline;
    }

    @Override
    protected UpdateByQueryRequest self() {
        return this;
    }

    @Override
    public UpdateByQueryRequest forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices) {
        UpdateByQueryRequest request = doForSlice(new UpdateByQueryRequest(slice, false), slicingTask, totalSlices);
        request.setPipeline(pipeline);
        return request;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("update-by-query ");
        searchToString(b);
        return b.toString();
    }

    //update by query updates all documents that match a query. The indices and indices options that affect how
    //indices are resolved depend entirely on the inner search request. That's why the following methods delegate to it.
    @Override
    public IndicesRequest indices(String... indices) {
        assert getSearchRequest() != null;
        getSearchRequest().indices(indices);
        return this;
    }

    @Override
    public String[] indices() {
        assert getSearchRequest() != null;
        return getSearchRequest().indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        assert getSearchRequest() != null;
        return getSearchRequest().indicesOptions();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        pipeline = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(pipeline);
    }
}
