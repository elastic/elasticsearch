/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.custommeta.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to put a search custom meta.
 */
public class PutCustomMetaRequest extends MasterNodeOperationRequest<PutCustomMetaRequest> {

    private String name;

    private SearchRequest searchRequest;

    PutCustomMetaRequest() {

    }

    /**
     * Constructs a new custom meta.
     *
     * @param name The name of the custom meta.
     */
    public PutCustomMetaRequest(String name) {
        this.name = name;
    }

    /**
     * Sets the name of the custom meta.
     */
    public PutCustomMetaRequest name(String name) {
        this.name = name;
        return this;
    }

    String name() {
        return this.name;
    }

    /**
     * Sets the search request to custom meta.
     */
    public PutCustomMetaRequest searchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
        return this;
    }

    /**
     * Sets the search request to custom meta.
     */
    public PutCustomMetaRequest searchRequest(SearchRequestBuilder searchRequest) {
        this.searchRequest = searchRequest.request();
        return this;
    }

    @Nullable
    SearchRequest searchRequest() {
        return this.searchRequest;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = searchRequest.validate();
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readString();
        if (in.readBoolean()) {
            searchRequest = new SearchRequest();
            searchRequest.readFrom(in);
        }
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
    }
}
