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

import org.elasticsearch.Version;
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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ObjectParser.fromList;

public final class FieldCapabilitiesRequest extends ActionRequest implements IndicesRequest.Replaceable {
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final String NAME = "field_caps_request";
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private String[] fields = Strings.EMPTY_ARRAY;
    // pkg private API mainly for cross cluster search to signal that we do multiple reductions ie. the results should not be merged
    private boolean mergeResults = true;

    private static ObjectParser<FieldCapabilitiesRequest, Void> PARSER =
        new ObjectParser<>(NAME, FieldCapabilitiesRequest::new);

    static {
        PARSER.declareStringArray(fromList(String.class, FieldCapabilitiesRequest::fields),
            FIELDS_FIELD);
    }

    public FieldCapabilitiesRequest() {}

    /**
     * Returns <code>true</code> iff the results should be merged.
     */
    boolean isMergeResults() {
        return mergeResults;
    }

    /**
     * if set to <code>true</code> the response will contain only a merged view of the per index field capabilities. Otherwise only
     * unmerged per index field capabilities are returned.
     */
    void setMergeResults(boolean mergeResults) {
        this.mergeResults = mergeResults;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        fields = in.readStringArray();
        if (in.getVersion().onOrAfter(Version.V_5_5_0)) {
            indices = in.readStringArray();
            indicesOptions = IndicesOptions.readIndicesOptions(in);
            mergeResults = in.readBoolean();
        } else {
            mergeResults = true;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(fields);
        if (out.getVersion().onOrAfter(Version.V_5_5_0)) {
            out.writeStringArray(indices);
            indicesOptions.writeIndicesOptions(out);
            out.writeBoolean(mergeResults);
        }
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
        Set<String> fieldSet = new HashSet<>(Arrays.asList(fields));
        this.fields = fieldSet.toArray(new String[0]);
        return this;
    }

    public String[] fields() {
        return fields;
    }

    /**
     *
     * The list of indices to lookup
     */
    public FieldCapabilitiesRequest indices(String... indices) {
        this.indices = Objects.requireNonNull(indices, "indices must not be null");
        return this;
    }

    public FieldCapabilitiesRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indices options must not be null");
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
