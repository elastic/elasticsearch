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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

/**
 * A multi percolate request that encapsulates multiple {@link PercolateRequest} instances in a single api call.
 */
public class MultiPercolateRequest extends ActionRequest<MultiPercolateRequest> implements CompositeIndicesRequest {

    private String[] indices;
    private String documentType;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosed();
    private List<PercolateRequest> requests = new ArrayList<>();

    /**
     * Embeds a percolate request to this multi percolate request
     */
    public MultiPercolateRequest add(PercolateRequestBuilder requestBuilder) {
        return add(requestBuilder.request());
    }

    /**
     * Embeds a percolate request to this multi percolate request
     */
    public MultiPercolateRequest add(PercolateRequest request) {
        if (request.indices() == null && indices != null) {
            request.indices(indices);
        }
        if (request.documentType() == null && documentType != null) {
            request.documentType(documentType);
        }
        if (request.indicesOptions() == IndicesOptions.strictExpandOpenAndForbidClosed() && indicesOptions != IndicesOptions.strictExpandOpenAndForbidClosed()) {
            request.indicesOptions(indicesOptions);
        }
        requests.add(request);
        return this;
    }

    /**
     * Embeds a percolate request which request body is defined as raw bytes to this multi percolate request
     */
    public MultiPercolateRequest add(byte[] data, int from, int length) throws Exception {
        return add(new BytesArray(data, from, length), true);
    }

    /**
     * Embeds a percolate request which request body is defined as raw bytes to this multi percolate request
     */
    public MultiPercolateRequest add(BytesReference data, boolean allowExplicitIndex) throws Exception {
        XContent xContent = XContentFactory.xContent(data);
        int from = 0;
        int length = data.length();
        byte marker = xContent.streamSeparator();
        while (true) {
            int nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }
            // support first line with \n
            if (nextMarker == 0) {
                from = nextMarker + 1;
                continue;
            }

            PercolateRequest percolateRequest = new PercolateRequest();
            if (indices != null) {
                percolateRequest.indices(indices);
            }
            if (documentType != null) {
                percolateRequest.documentType(documentType);
            }
            if (indicesOptions != IndicesOptions.strictExpandOpenAndForbidClosed()) {
                percolateRequest.indicesOptions(indicesOptions);
            }

            // now parse the action
            if (nextMarker - from > 0) {
                try (XContentParser parser = xContent.createParser(data.slice(from, nextMarker - from))) {
                    // Move to START_OBJECT, if token is null, its an empty data
                    XContentParser.Token token = parser.nextToken();
                    if (token != null) {
                        // Top level json object
                        assert token == XContentParser.Token.START_OBJECT;
                        token = parser.nextToken();
                        if (token != XContentParser.Token.FIELD_NAME) {
                            throw new ElasticsearchParseException("Expected field");
                        }
                        token = parser.nextToken();
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ElasticsearchParseException("expected start object");
                        }
                        String percolateAction = parser.currentName();
                        if ("percolate".equals(percolateAction)) {
                            parsePercolateAction(parser, percolateRequest, allowExplicitIndex);
                        } else if ("count".equals(percolateAction)) {
                            percolateRequest.onlyCount(true);
                            parsePercolateAction(parser, percolateRequest, allowExplicitIndex);
                        } else {
                            throw new ElasticsearchParseException("[{}] isn't a supported percolate operation", percolateAction);
                        }
                    }
                }
            }

            // move pointers
            from = nextMarker + 1;

            // now for the body
            nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }

            percolateRequest.source(data.slice(from, nextMarker - from));
            // move pointers
            from = nextMarker + 1;

            add(percolateRequest);
        }

        return this;
    }

    @Override
    public List<? extends IndicesRequest> subRequests() {
        List<IndicesRequest> indicesRequests = new ArrayList<>();
        for (PercolateRequest percolateRequest : this.requests) {
            indicesRequests.addAll(percolateRequest.subRequests());
        }
        return indicesRequests;
    }

    private void parsePercolateAction(XContentParser parser, PercolateRequest percolateRequest, boolean allowExplicitIndex) throws IOException {
        String globalIndex = indices != null && indices.length > 0 ? indices[0] : null;

        Map<String, Object> header = parser.map();

        if (header.containsKey("id")) {
            GetRequest getRequest = new GetRequest(globalIndex);
            percolateRequest.getRequest(getRequest);
            for (Map.Entry<String, Object> entry : header.entrySet()) {
                Object value = entry.getValue();
                if ("id".equals(entry.getKey())) {
                    getRequest.id(nodeStringValue(value, null));
                    header.put("id", entry.getValue());
                } else if ("index".equals(entry.getKey()) || "indices".equals(entry.getKey())) {
                    if (!allowExplicitIndex) {
                        throw new IllegalArgumentException("explicit index in multi percolate is not allowed");
                    }
                    getRequest.index(nodeStringValue(value, null));
                } else if ("type".equals(entry.getKey())) {
                    getRequest.type(nodeStringValue(value, null));
                } else if ("preference".equals(entry.getKey())) {
                    getRequest.preference(nodeStringValue(value, null));
                } else if ("routing".equals(entry.getKey())) {
                    getRequest.routing(nodeStringValue(value, null));
                } else if ("percolate_index".equals(entry.getKey()) || "percolate_indices".equals(entry.getKey()) || "percolateIndex".equals(entry.getKey()) || "percolateIndices".equals(entry.getKey())) {
                    percolateRequest.indices(nodeStringArrayValue(value));
                } else if ("percolate_type".equals(entry.getKey()) || "percolateType".equals(entry.getKey())) {
                    percolateRequest.documentType(nodeStringValue(value, null));
                } else if ("percolate_preference".equals(entry.getKey()) || "percolatePreference".equals(entry.getKey())) {
                    percolateRequest.preference(nodeStringValue(value, null));
                } else if ("percolate_routing".equals(entry.getKey()) || "percolateRouting".equals(entry.getKey())) {
                    percolateRequest.routing(nodeStringValue(value, null));
                }
            }

            // Setting values based on get request, if needed...
            if ((percolateRequest.indices() == null || percolateRequest.indices().length == 0) && getRequest.index() != null) {
                percolateRequest.indices(getRequest.index());
            }
            if (percolateRequest.documentType() == null && getRequest.type() != null) {
                percolateRequest.documentType(getRequest.type());
            }
            if (percolateRequest.routing() == null && getRequest.routing() != null) {
                percolateRequest.routing(getRequest.routing());
            }
            if (percolateRequest.preference() == null && getRequest.preference() != null) {
                percolateRequest.preference(getRequest.preference());
            }
        } else {
            for (Map.Entry<String, Object> entry : header.entrySet()) {
                Object value = entry.getValue();
                if ("index".equals(entry.getKey()) || "indices".equals(entry.getKey())) {
                    if (!allowExplicitIndex) {
                        throw new IllegalArgumentException("explicit index in multi percolate is not allowed");
                    }
                    percolateRequest.indices(nodeStringArrayValue(value));
                } else if ("type".equals(entry.getKey())) {
                    percolateRequest.documentType(nodeStringValue(value, null));
                } else if ("preference".equals(entry.getKey())) {
                    percolateRequest.preference(nodeStringValue(value, null));
                } else if ("routing".equals(entry.getKey())) {
                    percolateRequest.routing(nodeStringValue(value, null));
                }
            }
        }
        percolateRequest.indicesOptions(IndicesOptions.fromMap(header, indicesOptions));
    }

    private int findNextMarker(byte marker, int from, BytesReference data, int length) {
        for (int i = from; i < length; i++) {
            if (data.get(i) == marker) {
                return i;
            }
        }
        return -1;
    }

    /**
     * @return The list of already set percolate requests.
     */
    public List<PercolateRequest> requests() {
        return this.requests;
    }

    /**
     * @return Returns the {@link IndicesOptions} that is used as default for all percolate requests.
     */
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Sets the {@link IndicesOptions} for all percolate request that don't have this set.
     *
     * Warning: This should be set before adding any percolate requests. Setting this after adding percolate requests
     * will have no effect on any percolate requests already added.
     */
    public MultiPercolateRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * @return The default indices for all percolate request.
     */
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the default indices for any percolate request that doesn't have indices defined.
     *
     * Warning: This should be set before adding any percolate requests. Setting this after adding percolate requests
     * will have no effect on any percolate requests already added.
     */
    public MultiPercolateRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * @return Sets the default type for all percolate requests
     */
    public String documentType() {
        return documentType;
    }

    /**
     * Sets the default document type for any percolate request that doesn't have a document type set.
     *
     * Warning: This should be set before adding any percolate requests. Setting this after adding percolate requests
     * will have no effect on any percolate requests already added.
     */
    public MultiPercolateRequest documentType(String type) {
        this.documentType = type;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (int i = 0; i < requests.size(); i++) {
            ActionRequestValidationException ex = requests.get(i).validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }

        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        documentType = in.readOptionalString();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            PercolateRequest request = new PercolateRequest();
            request.readFrom(in);
            requests.add(request);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        out.writeOptionalString(documentType);
        indicesOptions.writeIndicesOptions(out);
        out.writeVInt(requests.size());
        for (PercolateRequest request : requests) {
            request.writeTo(out);
        }
    }
}
