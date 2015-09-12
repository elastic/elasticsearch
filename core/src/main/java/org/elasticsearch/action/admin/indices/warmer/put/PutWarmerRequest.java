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

package org.elasticsearch.action.admin.indices.warmer.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request that associates a {@link SearchRequest} with a name in the cluster that is
 * in-turn used to warm up indices before they are available for search.
 *
 * Note: neither the search request nor the name must be <code>null</code>
 */
public class PutWarmerRequest extends AcknowledgedRequest<PutWarmerRequest> implements IndicesRequest.Replaceable {

    private String name;

    private SearchRequest searchRequest;

    public PutWarmerRequest() {
    }

    /**
     * Constructs a new warmer.
     *
     * @param name The name of the warmer.
     */
    public PutWarmerRequest(String name) {
        this.name = name;
    }

    /**
     * Sets the name of the warmer.
     */
    public PutWarmerRequest name(String name) {
        this.name = name;
        return this;
    }

    public String name() {
        return this.name;
    }

    /**
     * Sets the search request to warm.
     */
    public PutWarmerRequest searchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
        return this;
    }

    /**
     * Sets the search request to warm.
     */
    public PutWarmerRequest searchRequest(SearchRequestBuilder searchRequest) {
        this.searchRequest = searchRequest.request();
        return this;
    }

    public SearchRequest searchRequest() {
        return this.searchRequest;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (searchRequest == null) {
            validationException = addValidationError("search request is missing", validationException);
        } else {
            validationException = searchRequest.validate();
        }
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        if (searchRequest == null) {
            throw new IllegalStateException("unable to retrieve indices, search request is null");
        }
        return searchRequest.indices();
    }

    @Override
    public IndicesRequest indices(String[] indices) {
        if (searchRequest == null) {
            throw new IllegalStateException("unable to set indices, search request is null");
        }
        searchRequest.indices(indices);
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        if (searchRequest == null) {
            throw new IllegalStateException("unable to retrieve indices options, search request is null");
        }
        return searchRequest.indicesOptions();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readString();
        if (in.readBoolean()) {
            searchRequest = new SearchRequest();
            searchRequest.readFrom(in);
        }
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        if (searchRequest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            searchRequest.writeTo(out);
        }
        writeTimeout(out);
    }
}
