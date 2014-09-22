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

package org.elasticsearch.action.admin.indices.mapping.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents a request to delete a mapping
 */
public class DeleteMappingRequest extends AcknowledgedRequest<DeleteMappingRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, false);
    private String[] types;

    DeleteMappingRequest() {
    }

    /**
     * Constructs a new delete mapping request against one or more indices. If nothing is set then
     * it will be executed against all indices.
     */
    public DeleteMappingRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(types)) {
            validationException = addValidationError("mapping type is missing", validationException);
        } else {
            validationException = checkForEmptyString(validationException, types);
        }
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index is missing", validationException);
        } else {
            validationException = checkForEmptyString(validationException, indices);
        }

        return validationException;
    }

    private ActionRequestValidationException checkForEmptyString(ActionRequestValidationException validationException, String[] strings) {
        boolean containsEmptyString = false;
        for (String string : strings) {
            if (!Strings.hasText(string)) {
                containsEmptyString = true;
            }
        }
        if (containsEmptyString) {
            validationException = addValidationError("types must not contain empty strings", validationException);
        }
        return validationException;
    }

    /**
     * Sets the indices this delete mapping operation will execute on.
     */
    @Override
    public DeleteMappingRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    /**
     * The indices the mappings will be removed from.
     */
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public DeleteMappingRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * The mapping types.
     */
    public String[] types() {
        return types;
    }

    /**
     * The type of the mappings to remove.
     */
    public DeleteMappingRequest types(String... types) {
        this.types = types;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        indicesOptions =  IndicesOptions.readIndicesOptions(in);
        types = in.readStringArray();
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeStringArrayNullable(types);
        writeTimeout(out);
    }
}
