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
package org.elasticsearch.action.percolate;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 *
 */
public class PercolateRequest extends BroadcastOperationRequest<PercolateRequest> implements CompositeIndicesRequest {

    private String documentType;
    private String routing;
    private String preference;
    private GetRequest getRequest;
    private boolean onlyCount;

    private BytesReference source;
    private boolean unsafe;

    private BytesReference docSource;

    // Used internally in order to compute tookInMillis, TransportBroadcastOperationAction itself doesn't allow
    // to hold it temporarily in an easy way
    long startTime;

    public PercolateRequest() {
    }

    PercolateRequest(PercolateRequest request, BytesReference docSource) {
        super(request);
        this.indices = request.indices();
        this.documentType = request.documentType();
        this.routing = request.routing();
        this.preference = request.preference();
        this.source = request.source;
        this.docSource = docSource;
        this.onlyCount = request.onlyCount;
        this.startTime = request.startTime;
    }

    @Override
    public List<? extends IndicesRequest> subRequests() {
        List<IndicesRequest> requests = Lists.newArrayList();
        requests.add(this);
        if (getRequest != null) {
            requests.add(getRequest);
        }
        return requests;
    }

    public String documentType() {
        return documentType;
    }

    public PercolateRequest documentType(String type) {
        this.documentType = type;
        return this;
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

    public GetRequest getRequest() {
        return getRequest;
    }

    public PercolateRequest getRequest(GetRequest getRequest) {
        this.getRequest = getRequest;
        return this;
    }

    /**
     * Before we fork on a local thread, make sure we copy over the bytes if they are unsafe
     */
    @Override
    public void beforeLocalFork() {
        if (unsafe) {
            source = source.copyBytesArray();
            unsafe = false;
        }
    }

    public BytesReference source() {
        return source;
    }

    public PercolateRequest source(Map document) throws ElasticsearchGenerationException {
        return source(document, Requests.CONTENT_TYPE);
    }

    @SuppressWarnings("unchecked")
    public PercolateRequest source(Map document, XContentType contentType) throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(document);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + document + "]", e);
        }
    }

    public PercolateRequest source(String document) {
        this.source = new BytesArray(document);
        this.unsafe = false;
        return this;
    }

    public PercolateRequest source(XContentBuilder documentBuilder) {
        source = documentBuilder.bytes();
        unsafe = false;
        return this;
    }

    public PercolateRequest source(byte[] document) {
        return source(document, 0, document.length);
    }

    public PercolateRequest source(byte[] source, int offset, int length) {
        return source(source, offset, length, false);
    }

    public PercolateRequest source(byte[] source, int offset, int length, boolean unsafe) {
        return source(new BytesArray(source, offset, length), unsafe);
    }

    public PercolateRequest source(BytesReference source, boolean unsafe) {
        this.source = source;
        this.unsafe = unsafe;
        return this;
    }

    public PercolateRequest source(PercolateSourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        this.unsafe = false;
        return this;
    }

    public boolean onlyCount() {
        return onlyCount;
    }

    public PercolateRequest onlyCount(boolean onlyCount) {
        this.onlyCount = onlyCount;
        return this;
    }

    BytesReference docSource() {
        return docSource;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (documentType == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (source == null && getRequest == null) {
            validationException = addValidationError("source or get is missing", validationException);
        }
        if (getRequest != null && getRequest.fields() != null) {
            validationException = addValidationError("get fields option isn't supported via percolate request", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        startTime = in.readVLong();
        documentType = in.readString();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        unsafe = false;
        source = in.readBytesReference();
        docSource = in.readBytesReference();
        if (in.readBoolean()) {
            getRequest = new GetRequest(null);
            getRequest.readFrom(in);
        }
        onlyCount = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(startTime);
        out.writeString(documentType);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeBytesReference(source);
        out.writeBytesReference(docSource);
        if (getRequest != null) {
            out.writeBoolean(true);
            getRequest.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(onlyCount);
    }
}
