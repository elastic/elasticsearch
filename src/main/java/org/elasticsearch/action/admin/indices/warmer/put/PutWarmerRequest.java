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

package org.elasticsearch.action.admin.indices.warmer.put;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.unit.TimeValue.readTimeValue;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 * A request to put a search warmer.
 */
public class PutWarmerRequest extends MasterNodeOperationRequest<PutWarmerRequest>
        implements AcknowledgedRequest<PutWarmerRequest> {

    private String name;

    private SearchRequest searchRequest;

    private TimeValue timeout = timeValueSeconds(10);

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

    @Nullable
    SearchRequest searchRequest() {
        return this.searchRequest;
    }

    @Override
    public PutWarmerRequest timeout(String timeout) {
        return this;
    }

    @Override
    public PutWarmerRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public TimeValue timeout() {
        return timeout;
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
        if (in.getVersion().onOrAfter(Version.V_0_90_6)) {
            timeout = readTimeValue(in);
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
        if (out.getVersion().onOrAfter(Version.V_0_90_6)) {
            timeout.writeTo(out);
        }
    }
}
