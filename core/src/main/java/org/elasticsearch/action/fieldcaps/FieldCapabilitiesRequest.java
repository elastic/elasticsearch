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

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.common.xcontent.ObjectParser.fromList;

public class FieldCapabilitiesRequest extends ActionRequest implements IndicesRequest {
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final String NAME = "field_caps_request";
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private String[] fields = Strings.EMPTY_ARRAY;

    private static ObjectParser<FieldCapabilitiesRequest, Void> PARSER = new ObjectParser<>(NAME);

    static {
        PARSER.declareStringArray(fromList(String.class, FieldCapabilitiesRequest::fields),
            FIELDS_FIELD);
    }

    public FieldCapabilitiesRequest() {}

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        fields = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(fields);
    }

    public static FieldCapabilitiesRequest parseFields(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * The list of field names to retrieve
     */
    public FieldCapabilitiesRequest fields(String... fields) {
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
     *
     * The list of indices to lookup
     */
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
            validationException =
                ValidateActions.addValidationError("no fields specified", validationException);
        }
        return validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldCapabilitiesRequest that = (FieldCapabilitiesRequest) o;

        if (!Arrays.equals(indices, that.indices)) return false;
        if (!indicesOptions.equals(that.indicesOptions)) return false;
        return Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(indices);
        result = 31 * result + indicesOptions.hashCode();
        result = 31 * result + Arrays.hashCode(fields);
        return result;
    }
}
