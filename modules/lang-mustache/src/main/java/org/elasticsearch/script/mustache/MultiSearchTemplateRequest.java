/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class MultiSearchTemplateRequest extends LegacyActionRequest implements CompositeIndicesRequest {

    private int maxConcurrentSearchRequests = 0;
    private List<SearchTemplateRequest> requests = new ArrayList<>();

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled();

    public MultiSearchTemplateRequest() {}

    MultiSearchTemplateRequest(StreamInput in) throws IOException {
        super(in);
        maxConcurrentSearchRequests = in.readVInt();
        requests = in.readCollectionAsList(SearchTemplateRequest::new);
    }

    /**
     * Add a search template request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchTemplateRequest add(SearchTemplateRequestBuilder request) {
        requests.add(request.request());
        return this;
    }

    /**
     * Add a search template request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchTemplateRequest add(SearchTemplateRequest request) {
        requests.add(request);
        return this;
    }

    /**
     * Returns the amount of search requests specified in this multi search requests are allowed to be ran concurrently.
     */
    public int maxConcurrentSearchRequests() {
        return maxConcurrentSearchRequests;
    }

    /**
     * Sets how many search requests specified in this multi search requests are allowed to be ran concurrently.
     */
    public MultiSearchTemplateRequest maxConcurrentSearchRequests(int maxConcurrentSearchRequests) {
        if (maxConcurrentSearchRequests < 1) {
            throw new IllegalArgumentException("maxConcurrentSearchRequests must be positive");
        }

        this.maxConcurrentSearchRequests = maxConcurrentSearchRequests;
        return this;
    }

    public List<SearchTemplateRequest> requests() {
        return this.requests;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (SearchTemplateRequest request : requests) {
            ActionRequestValidationException ex = request.validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }
        return validationException;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public MultiSearchTemplateRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(maxConcurrentSearchRequests);
        out.writeCollection(requests);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiSearchTemplateRequest that = (MultiSearchTemplateRequest) o;
        return maxConcurrentSearchRequests == that.maxConcurrentSearchRequests
            && Objects.equals(requests, that.requests)
            && Objects.equals(indicesOptions, that.indicesOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxConcurrentSearchRequests, requests, indicesOptions);
    }

    public static byte[] writeMultiLineFormat(MultiSearchTemplateRequest multiSearchTemplateRequest, XContent xContent) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (SearchTemplateRequest templateRequest : multiSearchTemplateRequest.requests()) {
            final SearchRequest searchRequest = templateRequest.getRequest();
            try (XContentBuilder xContentBuilder = XContentBuilder.builder(xContent)) {
                MultiSearchRequest.writeSearchRequestParams(searchRequest, xContentBuilder);
                BytesReference.bytes(xContentBuilder).writeTo(output);
            }
            output.write(xContent.bulkSeparator());
            try (XContentBuilder xContentBuilder = XContentBuilder.builder(xContent)) {
                templateRequest.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                BytesReference.bytes(xContentBuilder).writeTo(output);
            }
            output.write(xContent.bulkSeparator());
        }
        return output.toByteArray();
    }

}
