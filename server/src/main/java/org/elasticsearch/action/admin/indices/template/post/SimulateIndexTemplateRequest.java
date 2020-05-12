/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateV2Action;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class SimulateIndexTemplateRequest extends MasterNodeReadRequest<SimulateIndexTemplateRequest> {

    private String indexName;

    @Nullable
    private PutIndexTemplateV2Action.Request indexTemplateRequest;

    public SimulateIndexTemplateRequest(String indexName) {
        if (Strings.isNullOrEmpty(indexName)) {
            throw new IllegalArgumentException("index name cannot be null or empty");
        }
        this.indexName = indexName;
    }

    public SimulateIndexTemplateRequest(StreamInput in) throws IOException {
        super(in);
        indexName = in.readString();
        indexTemplateRequest = in.readOptionalWriteable(PutIndexTemplateV2Action.Request::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeOptionalWriteable(indexTemplateRequest);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexTemplateRequest != null) {
            validationException = indexTemplateRequest.validateIndexTemplate(validationException);
        }
        return validationException;
    }

    public String getIndexName() {
        return indexName;
    }

    @Nullable
    public PutIndexTemplateV2Action.Request getIndexTemplateRequest() {
        return indexTemplateRequest;
    }

    public SimulateIndexTemplateRequest indexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public SimulateIndexTemplateRequest indexTemplateRequest(PutIndexTemplateV2Action.Request indexTemplateRequest) {
        this.indexTemplateRequest = indexTemplateRequest;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimulateIndexTemplateRequest that = (SimulateIndexTemplateRequest) o;
        return indexName.equals(that.indexName) &&
            Objects.equals(indexTemplateRequest, that.indexTemplateRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, indexTemplateRequest);
    }
}
