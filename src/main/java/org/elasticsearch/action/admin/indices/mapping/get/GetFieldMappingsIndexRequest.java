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

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.custom.SingleCustomOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

class GetFieldMappingsIndexRequest extends SingleCustomOperationRequest<GetFieldMappingsIndexRequest> {

    private String index;

    private boolean probablySingleFieldRequest;
    private boolean includeDefaults;
    private String[] fields = Strings.EMPTY_ARRAY;
    private String[] types = Strings.EMPTY_ARRAY;

    GetFieldMappingsIndexRequest() {
    }

    GetFieldMappingsIndexRequest(GetFieldMappingsRequest other, String index, boolean probablySingleFieldRequest) {
        this.preferLocal(other.local);
        this.probablySingleFieldRequest = probablySingleFieldRequest;
        this.includeDefaults = other.includeDefaults();
        this.types = other.types();
        this.fields = other.fields();
        this.index = index;
    }

    public String index() {
        return index;
    }

    public String[] types() {
        return types;
    }

    public String[] fields() {
        return fields;
    }

    public boolean probablySingleFieldRequest() {
        return probablySingleFieldRequest;
    }

    public boolean includeDefaults() {
        return includeDefaults;
    }

    /** Indicates whether default mapping settings should be returned */
    public GetFieldMappingsIndexRequest includeDefaults(boolean includeDefaults) {
        this.includeDefaults = includeDefaults;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeStringArray(types);
        out.writeStringArray(fields);
        out.writeBoolean(includeDefaults);
        out.writeBoolean(probablySingleFieldRequest);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        types = in.readStringArray();
        fields = in.readStringArray();
        includeDefaults = in.readBoolean();
        probablySingleFieldRequest = in.readBoolean();
    }
}
