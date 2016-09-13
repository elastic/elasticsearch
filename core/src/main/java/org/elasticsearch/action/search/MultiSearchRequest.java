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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A multi search API request.
 */
public class MultiSearchRequest extends ActionRequest<MultiSearchRequest> implements CompositeIndicesRequest {

    private int maxConcurrentSearchRequests = 0;
    private List<SearchRequest> requests = new ArrayList<>();

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosed();

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchRequest add(SearchRequestBuilder request) {
        requests.add(request.request());
        return this;
    }

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchRequest add(SearchRequest request) {
        requests.add(request);
        return this;
    }

    /**
     * Returns the amount of search requests specified in this multi search requests are allowed to be ran concurrently.
     */
    public int maxConcurrentSearchRequests() {
        return maxConcurrentSearchRequests;
    }

    /**
     * Sets how many search requests specified in this multi search requests are allowed to be ran concurrently.
     */
    public MultiSearchRequest maxConcurrentSearchRequests(int maxConcurrentSearchRequests) {
        if (maxConcurrentSearchRequests < 1) {
            throw new IllegalArgumentException("maxConcurrentSearchRequests must be positive");
        }

        this.maxConcurrentSearchRequests = maxConcurrentSearchRequests;
        return this;
    }

    public List<SearchRequest> requests() {
        return this.requests;
    }

    @Override
    public List<? extends IndicesRequest> subRequests() {
        return this.requests;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (int i = 0; i < requests.size(); i++) {
            ActionRequestValidationException ex = requests.get(i).validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }

        return validationException;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public MultiSearchRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        maxConcurrentSearchRequests = in.readVInt();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            SearchRequest request = new SearchRequest();
            request.readFrom(in);
            requests.add(request);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(maxConcurrentSearchRequests);
        out.writeVInt(requests.size());
        for (SearchRequest request : requests) {
            request.writeTo(out);
        }
    }
}
