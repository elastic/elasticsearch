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

import org.elasticsearch.action.search.SearchRequest;

/**
 * Request to reindex a set of documents where they are without changing their
 * locations or IDs.
 */
public class UpdateByQueryRequest extends AbstractBulkIndexByScrollRequest<UpdateByQueryRequest> {
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
}
