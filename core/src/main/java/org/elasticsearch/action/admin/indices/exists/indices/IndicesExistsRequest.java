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

package org.elasticsearch.action.admin.indices.exists.indices;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class IndicesExistsRequest extends MasterNodeReadRequest<IndicesExistsRequest> implements IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, true);

    // for serialization
    public IndicesExistsRequest() {

    }

    public IndicesExistsRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesExistsRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public IndicesExistsRequest expandWilcardsOpen(boolean expandWildcardsOpen) {
        this.indicesOptions = IndicesOptions.fromOptions(indicesOptions.ignoreUnavailable(), indicesOptions.allowNoIndices(),
                expandWildcardsOpen, indicesOptions.expandWildcardsClosed());
        return this;
    }

    public IndicesExistsRequest expandWilcardsClosed(boolean expandWildcardsClosed) {
        this.indicesOptions = IndicesOptions.fromOptions(indicesOptions.ignoreUnavailable(), indicesOptions.allowNoIndices(),
                indicesOptions.expandWildcardsOpen(), expandWildcardsClosed);
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null || indices.length == 0) {
            validationException = addValidationError("index/indices is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }
}
