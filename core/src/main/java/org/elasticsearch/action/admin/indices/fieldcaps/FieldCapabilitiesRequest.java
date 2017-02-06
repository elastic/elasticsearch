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

package org.elasticsearch.action.admin.indices.fieldcaps;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * Request the capabilities of specific fields.
 */
public class FieldCapabilitiesRequest extends ActionRequest implements IndicesRequest {
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private String[] fields = Strings.EMPTY_ARRAY;
    private String level = "cluster";

    public FieldCapabilitiesRequest() {
    }

    /**
     * The list of field names to retrieve.
     */
    public FieldCapabilitiesRequest fields(String[] fields) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("specified fields can't be null or empty");
        }
        this.fields = fields;
        return this;
    }

    public String[] fields() {
        return fields;
    }

    /**
     * Whether the field capabilities should be expanded per index or merged in a single index named "_all"
     */
    public String level() {
        return level;
    }

    public FieldCapabilitiesRequest level(String level) {
        this.level = level;
        return this;
    }

    public FieldCapabilitiesRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    public FieldCapabilitiesRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (fields == null || fields.length == 0) {
            validationException = ValidateActions.addValidationError("no fields specified", validationException);
        }
        if ("cluster".equals(level) == false && "indices".equals(level) == false) {
            validationException =
                ValidateActions.addValidationError("invalid level option [" + level + "]", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        fields = in.readStringArray();
        level = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(fields);
        out.writeString(level);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldCapabilitiesRequest that = (FieldCapabilitiesRequest) o;

        if (!Arrays.equals(indices, that.indices)) return false;
        if (!indicesOptions.equals(that.indicesOptions)) return false;
        if (!Arrays.equals(fields, that.fields)) return false;
        return level.equals(that.level);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(indices);
        result = 31 * result + indicesOptions.hashCode();
        result = 31 * result + Arrays.hashCode(fields);
        result = 31 * result + level.hashCode();
        return result;
    }
}
