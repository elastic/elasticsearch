/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public class GetIndexTemplatesRequest extends MasterNodeOperationRequest<GetIndexTemplatesRequest> {

    private String[] names;

    public GetIndexTemplatesRequest() {}

    @Deprecated
    public GetIndexTemplatesRequest(String name) {
        this.names = new String[1];
        this.names[0] = name;
    }

    public GetIndexTemplatesRequest(String... names) {
        this.names = names;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Sets the name of the index template.
     */
    @Deprecated
    public GetIndexTemplatesRequest name(String name) {
        this.names = new String[1];
        this.names[0] = name;
        return this;
    }

    /**
     * The name of the index template.
     */
    @Deprecated
    public String name() {
        if (this.names != null && this.names.length > 0) {
            return this.names[0];
        }
        return null;
    }

    /**
     * Sets the names of the index templates.
     */
    public GetIndexTemplatesRequest names(String... names) {
        this.names = names;
        return this;
    }

    /**
     * The names of the index templates.
     */
    public String[] names() {
        return this.names;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        names = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(names);
    }
}
