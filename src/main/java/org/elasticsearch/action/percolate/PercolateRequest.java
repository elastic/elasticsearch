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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to execute a percolate operation.
 */
public class PercolateRequest extends BroadcastOperationRequest<PercolateRequest> implements CompositeIndicesRequest {

    private String documentType;
    private String routing;
    private String preference;
    private GetRequest getRequest;
    private boolean onlyCount;

    private BytesReference source;

    private BytesReference docSource;

    // Used internally in order to compute tookInMillis, TransportBroadcastOperationAction itself doesn't allow
    // to hold it temporarily in an easy way
    long startTime;

    /**
     * Constructor only for internal usage.
     */
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

    /**
     * Getter for {@link #documentType(String)}
     */
    public String documentType() {
        return documentType;
    }

    /**
     * Sets the type of the document to percolate. This is important as it selects the mapping to be used to parse
     * the document.
     */
    public PercolateRequest documentType(String type) {
        this.documentType = type;
        return this;
    }

    /**
     * Getter for {@link #routing(String)}
     */
    public String routing() {
        return routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public PercolateRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Getter for {@link #preference(String)}
     */
    public String preference() {
        return preference;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public PercolateRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    /**
     * Getter for {@link #getRequest(GetRequest)}
     */
    public GetRequest getRequest() {
        return getRequest;
    }

    /**
     * This defines where to fetch the document to be percolated from, which is an alternative of defining the document
     * to percolate in the request body.
     *
     * If this defined than this will override the document specified in the request body.
     */
    public PercolateRequest getRequest(GetRequest getRequest) {
        this.getRequest = getRequest;
        return this;
    }

    /**
     * @return The request body in its raw form.
     */
    public BytesReference source() {
        return source;
    }

    /**
     * Raw version of {@link #source(PercolateSourceBuilder)}
     */
    public PercolateRequest source(Map document) throws ElasticsearchGenerationException {
        return source(document, Requests.CONTENT_TYPE);
    }

    /**
     * Raw version of {@link #source(PercolateSourceBuilder)}
     */
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

    /**
     * Raw version of {@link #source(PercolateSourceBuilder)}
     */
    public PercolateRequest source(String document) {
        this.source = new BytesArray(document);
        return this;
    }

    /**
     * Raw version of {@link #source(PercolateSourceBuilder)}
     */
    public PercolateRequest source(XContentBuilder documentBuilder) {
        source = documentBuilder.bytes();
        return this;
    }

    /**
     * Raw version of {@link #source(PercolateSourceBuilder)}
     */
    public PercolateRequest source(byte[] document) {
        return source(document, 0, document.length);
    }

    /**
     * Raw version of {@link #source(PercolateSourceBuilder)}
     */
    public PercolateRequest source(byte[] source, int offset, int length) {
        return source(new BytesArray(source, offset, length));
    }

    /**
     * Raw version of {@link #source(PercolateSourceBuilder)}
     */
    public PercolateRequest source(BytesReference source) {
        this.source = source;
        return this;
    }

    /**
     * Sets the request body definition for this percolate request as raw bytes.
     *
     * This is the preferred way to set the request body.
     */
    public PercolateRequest source(PercolateSourceBuilder sourceBuilder) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            sourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            this.source = builder.bytes();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
        return this;
    }

    /**
     * Getter for {@link #onlyCount(boolean)}
     */
    public boolean onlyCount() {
        return onlyCount;
    }

    /**
     * Sets whether this percolate request should only count the number of percolator queries that matches with
     * the document being percolated and don't keep track of the actual queries that have matched.
     */
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
