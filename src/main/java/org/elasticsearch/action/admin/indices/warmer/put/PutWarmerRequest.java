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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to put a search warmer.
 */
public class PutWarmerRequest extends AcknowledgedRequest<PutWarmerRequest> {

    private String name;

    private SearchRequest searchRequest;

    PutWarmerRequest() {
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

    String name() {
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

    SearchRequest searchRequest() {
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
