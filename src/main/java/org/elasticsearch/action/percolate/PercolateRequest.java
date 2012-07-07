/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.percolate;

import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.custom.SingleCustomOperationRequest;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 *
 */
public class PercolateRequest extends SingleCustomOperationRequest {

    private String index;
    private String type;

    private BytesReference source;
    private boolean sourceUnsafe;

    public PercolateRequest() {

    }

    /**
     * Constructs a new percolate request.
     *
     * @param index The index name
     * @param type  The document type
     */
    public PercolateRequest(String index, String type) {
        this.index = index;
        this.type = type;
    }

    public PercolateRequest index(String index) {
        this.index = index;
        return this;
    }

    public PercolateRequest type(String type) {
        this.type = type;
        return this;
    }

    public String index() {
        return this.index;
    }

    public String type() {
        return this.type;
    }

    /**
     * Before we fork on a local thread, make sure we copy over the bytes if they are unsafe
     */
    @Override
    public void beforeLocalFork() {
        if (sourceUnsafe) {
            source = source.copyBytesArray();
            sourceUnsafe = false;
        }
    }

    public BytesReference source() {
        return source;
    }

    @Required
    public PercolateRequest source(Map source) throws ElasticSearchGenerationException {
        return source(source, XContentType.SMILE);
    }

    @Required
    public PercolateRequest source(Map source, XContentType contentType) throws ElasticSearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    @Required
    public PercolateRequest source(String source) {
        this.source = new BytesArray(source);
        this.sourceUnsafe = false;
        return this;
    }

    @Required
    public PercolateRequest source(XContentBuilder sourceBuilder) {
        source = sourceBuilder.bytes();
        sourceUnsafe = false;
        return this;
    }

    public PercolateRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    @Required
    public PercolateRequest source(byte[] source, int offset, int length) {
        return source(source, offset, length, false);
    }

    @Required
    public PercolateRequest source(byte[] source, int offset, int length, boolean unsafe) {
        return source(new BytesArray(source, offset, length), unsafe);
    }

    @Required
    public PercolateRequest source(BytesReference source, boolean unsafe) {
        this.source = source;
        this.sourceUnsafe = unsafe;
        return this;
    }

    /**
     * if this operation hits a node with a local relevant shard, should it be preferred
     * to be executed on, or just do plain round robin. Defaults to <tt>true</tt>
     */
    @Override
    public PercolateRequest preferLocal(boolean preferLocal) {
        super.preferLocal(preferLocal);
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readUTF();
        type = in.readUTF();

        sourceUnsafe = false;
        source = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeUTF(index);
        out.writeUTF(type);
        out.writeBytesReference(source);
    }
}
