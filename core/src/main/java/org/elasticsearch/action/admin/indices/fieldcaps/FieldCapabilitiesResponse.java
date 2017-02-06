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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Response for {@link FieldCapabilitiesRequest} and {@link FieldCapabilitiesIndexRequest} requests.
 */
public class FieldCapabilitiesResponse extends ActionResponse implements ToXContent {
    private Map<String, Map<String, FieldCapabilities>> fieldsCaps;

    FieldCapabilitiesResponse(Map<String, Map<String, FieldCapabilities> > fieldsCaps) {
        this.fieldsCaps = fieldsCaps;
    }

    FieldCapabilitiesResponse() {
    }

    /**
     * Returns the capabilities of each existing requested fields.
     * The map key is the field name and the value is a map of capabilities for the field,
     * with one entry per requested index if {@link FieldCapabilitiesRequest#level()} equals to "indices",
     * or a single entry named `_all` if {@link FieldCapabilitiesRequest#level()} equals to "cluster".
     */
    public Map<String, Map<String, FieldCapabilities> > getFieldsCaps() {
        return fieldsCaps;
    }

    /**
     * Returns a map of capabilities for the field, with one entry per requested index if
     * {@link FieldCapabilitiesRequest#level()} equals to "indices", or a single entry named `_all` if
     * {@link FieldCapabilitiesRequest#level()} equals to "cluster".
     */
    public Map<String, FieldCapabilities> getFieldCap(String field) {
        return fieldsCaps.get(field);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.fieldsCaps = in.readMap(StreamInput::readString, FieldCapabilitiesResponse::readFieldsCaps);
    }

    static Map<String, FieldCapabilities> readFieldsCaps(StreamInput in) throws IOException {
        return in.readMap(StreamInput::readString, FieldCapabilities::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(fieldsCaps, StreamOutput::writeString, FieldCapabilitiesResponse::writeFieldsCaps);
    }

    static void writeFieldsCaps(StreamOutput out, Map<String, FieldCapabilities> map) throws IOException {
        out.writeMap(map, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("fields", fieldsCaps);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldCapabilitiesResponse that = (FieldCapabilitiesResponse) o;
        return fieldsCaps.equals(that.fieldsCaps);
    }

    @Override
    public int hashCode() {
        return fieldsCaps.hashCode();
    }
}
