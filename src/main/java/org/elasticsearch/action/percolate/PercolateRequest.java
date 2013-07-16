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
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
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
public class PercolateRequest extends BroadcastOperationRequest<PercolateRequest> {

    private String documentType;
    private String routing;
    private String preference;

    private BytesReference documentSource;
    private boolean documentUnsafe;

    // Used internally in order to compute tookInMillis, TransportBroadcastOperationAction itself doesn't allow
    // to hold it temporarily in an easy way
    long startTime;

    PercolateRequest() {
    }

    public PercolateRequest(String index, String documentType) {
        super(new String[]{index});
        this.documentType = documentType;
    }

    public String documentType() {
        return documentType;
    }

    public void documentType(String type) {
        this.documentType = type;
    }

    public String routing() {
        return routing;
    }

    public PercolateRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    public String preference() {
        return preference;
    }

    public PercolateRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    /**
     * Before we fork on a local thread, make sure we copy over the bytes if they are unsafe
     */
    @Override
    public void beforeLocalFork() {
        if (documentUnsafe) {
            documentSource = documentSource.copyBytesArray();
            documentUnsafe = false;
        }
    }

    public BytesReference documentSource() {
        return documentSource;
    }

    @Required
    public PercolateRequest documentSource(Map document) throws ElasticSearchGenerationException {
        return documentSource(document, XContentType.SMILE);
    }

    @Required
    public PercolateRequest documentSource(Map document, XContentType contentType) throws ElasticSearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(document);
            return documentSource(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + document + "]", e);
        }
    }

    @Required
    public PercolateRequest documentSource(String document) {
        this.documentSource = new BytesArray(document);
        this.documentUnsafe = false;
        return this;
    }

    @Required
    public PercolateRequest documentSource(XContentBuilder documentBuilder) {
        documentSource = documentBuilder.bytes();
        documentUnsafe = false;
        return this;
    }

    public PercolateRequest documentSource(byte[] document) {
        return documentSource(document, 0, document.length);
    }

    @Required
    public PercolateRequest documentSource(byte[] source, int offset, int length) {
        return documentSource(source, offset, length, false);
    }

    @Required
    public PercolateRequest documentSource(byte[] source, int offset, int length, boolean unsafe) {
        return documentSource(new BytesArray(source, offset, length), unsafe);
    }

    @Required
    public PercolateRequest documentSource(BytesReference source, boolean unsafe) {
        this.documentSource = source;
        this.documentUnsafe = unsafe;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (indices == null || indices.length == 0) {
            validationException = addValidationError("index is missing", validationException);
        }
        if (documentType == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (documentSource == null) {
            validationException = addValidationError("documentSource is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        startTime = in.readVLong();
        documentType = in.readString();
        documentUnsafe = false;
        documentSource = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(startTime);
        out.writeString(documentType);
        out.writeBytesReference(documentSource);
    }
}
