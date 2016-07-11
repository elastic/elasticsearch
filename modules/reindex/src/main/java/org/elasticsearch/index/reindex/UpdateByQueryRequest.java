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

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * Request to update some documents. That means you can't change their type, id, index, or anything like that. This implements
 * CompositeIndicesRequest but in a misleading way. Rather than returning all the subrequests that it will make it tries to return a
 * representative set of subrequests. This is best-effort but better than {@linkplain ReindexRequest} because scripts can't change the
 * destination index and things.
 */
public class UpdateByQueryRequest extends AbstractBulkIndexByScrollRequest<UpdateByQueryRequest> implements CompositeIndicesRequest {
    /**
     * Ingest pipeline to set on index requests made by this action.
     */
    private String pipeline;

    public UpdateByQueryRequest() {
    }

    public UpdateByQueryRequest(SearchRequest search) {
        super(search);
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
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("update-by-query ");
        searchToString(b);
        return b.toString();
    }

    // CompositeIndicesRequest implementation so plugins can reason about the request. This is really just a best effort thing.
    /**
     * Accessor to get the underlying {@link IndicesRequest}s that this request wraps. Note that this method is <strong>not
     * accurate</strong> since it returns dummy {@link IndexRequest}s and not the actual requests that will be issued as part of the
     * execution of this request.
     *
     * @return a list comprising of the {@link SearchRequest} and dummy {@link IndexRequest}s
     */
    @Override
    public List<? extends IndicesRequest> subRequests() {
        assert getSearchRequest() != null;
        List<IndicesRequest> subRequests = new ArrayList<>();
        // One dummy IndexRequest per destination index.
        for (String index : getSearchRequest().indices()) {
            IndexRequest request = new IndexRequest();
            request.index(index);
            subRequests.add(request);
        }
        subRequests.add(getSearchRequest());
        return unmodifiableList(subRequests);
    };
}
