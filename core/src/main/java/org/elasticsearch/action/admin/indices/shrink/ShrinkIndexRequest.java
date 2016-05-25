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
package org.elasticsearch.action.admin.indices.shrink;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 */
public class ShrinkIndexRequest extends AcknowledgedRequest<ShrinkIndexRequest> implements IndicesRequest {

    private CreateIndexRequest createIndexRequest;
    private String sourceIndex = null;

    public ShrinkIndexRequest() {
    }

    public ShrinkIndexRequest(String targetIndex, String sourceindex) {
        this.createIndexRequest = new CreateIndexRequest(targetIndex);
        this.sourceIndex = sourceindex;
    }

    public ShrinkIndexRequest(CreateIndexRequest targetIndex, String sourceindex) {
        this.createIndexRequest = targetIndex;
        this.sourceIndex = sourceindex;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndex == null) {
            validationException = addValidationError("source index is missing", validationException);
        }
        if (createIndexRequest == null) {
            validationException = addValidationError("target index is missing", validationException);
        }
        return validationException;
    }

    public void setSourceIndex(String index) {
        this.sourceIndex = index;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        createIndexRequest = new CreateIndexRequest();
        createIndexRequest.readFrom(in);
        sourceIndex = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        createIndexRequest.writeTo(out);
        out.writeString(sourceIndex);
    }

    @Override
    public String[] indices() {
        return new String[0];
    }

    @Override
    public IndicesOptions indicesOptions() {
        return null;
    }

    public void setTargetIndex(String targetIndex) {
        this.createIndexRequest = new CreateIndexRequest(targetIndex);;
    }

    public void setTargetIndex(CreateIndexRequest targetIndexRequest) {
        this.createIndexRequest = targetIndexRequest;
    }

    public CreateIndexRequest getTargetIndex() {
        return createIndexRequest;
    }

    public String getSourceIndex() {
        return sourceIndex;
    }
}
