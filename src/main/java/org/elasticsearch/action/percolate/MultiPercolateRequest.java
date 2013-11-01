/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.action.percolate;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 */
public class MultiPercolateRequest extends ActionRequest<MultiPercolateRequest> {

    private String[] indices;
    private String documentType;
    private IgnoreIndices ignoreIndices = IgnoreIndices.DEFAULT;
    private List<PercolateRequest> requests = Lists.newArrayList();

    public MultiPercolateRequest add(PercolateRequestBuilder requestBuilder) {
        return add(requestBuilder.request());
    }

    public MultiPercolateRequest add(PercolateRequest request) {
        if (request.indices() == null && indices != null) {
            request.indices(indices);
        }
        if (request.documentType() == null && documentType != null) {
            request.documentType(documentType);
        }
        if (request.ignoreIndices() == IgnoreIndices.DEFAULT && ignoreIndices != IgnoreIndices.DEFAULT) {
            request.ignoreIndices(ignoreIndices);
        }
        requests.add(request);
        return this;
    }

    public MultiPercolateRequest add(byte[] data, int from, int length, boolean contentUnsafe) throws Exception {
        return add(new BytesArray(data, from, length), contentUnsafe);
    }

    public MultiPercolateRequest add(BytesReference data, boolean contentUnsafe) throws Exception {
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
            if (ignoreIndices != IgnoreIndices.DEFAULT) {
                percolateRequest.ignoreIndices(ignoreIndices);
            }

            // now parse the action
            if (nextMarker - from > 0) {
                XContentParser parser = xContent.createParser(data.slice(from, nextMarker - from));
                try {
                    // Move to START_OBJECT, if token is null, its an empty data
                    XContentParser.Token token = parser.nextToken();
                    if (token != null) {
                        // Top level json object
                        assert token == XContentParser.Token.START_OBJECT;
                        token = parser.nextToken();
                        if (token != XContentParser.Token.FIELD_NAME) {
                            throw new ElasticSearchParseException("Expected field");
                        }
                        token = parser.nextToken();
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ElasticSearchParseException("expected start object");
                        }
                        String percolateAction = parser.currentName();
                        if ("percolate".equals(percolateAction)) {
                            parsePercolateAction(parser, percolateRequest);
                        } else if ("count".equals(percolateAction)) {
                            percolateRequest.onlyCount(true);
                            parsePercolateAction(parser, percolateRequest);
                        } else {
                            throw new ElasticSearchParseException(percolateAction + " isn't a supported percolate operation");
                        }
                    }
                } finally {
                    parser.close();
                }
            }

            // move pointers
            from = nextMarker + 1;

            // now for the body
            nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }

            percolateRequest.source(data.slice(from, nextMarker - from), contentUnsafe);
            // move pointers
            from = nextMarker + 1;

            add(percolateRequest);
        }

        return this;
    }

    private void parsePercolateAction(XContentParser parser, PercolateRequest percolateRequest) throws IOException {
        String globalIndex = indices != null && indices.length > 0 ? indices[0] : null;

        Map<String, Object> header = new HashMap<String, Object>();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                header.put(currentFieldName, parser.text());
            } else if (token == XContentParser.Token.START_ARRAY) {
                header.put(currentFieldName, parseArray(parser));
            }
        }

        if (header.containsKey("id")) {
            GetRequest getRequest = new GetRequest(globalIndex);
            percolateRequest.getRequest(getRequest);
            for (Map.Entry<String, Object> entry : header.entrySet()) {
                Object value = entry.getValue();
                if ("id".equals(entry.getKey())) {
                    getRequest.id((String) value);
                    header.put("id", entry.getValue());
                } else if ("index".equals(entry.getKey()) || "indices".equals(entry.getKey())) {
                    getRequest.index((String) value);
                } else if ("type".equals(entry.getKey())) {
                    getRequest.type((String) value);
                } else if ("preference".equals(entry.getKey())) {
                    getRequest.preference((String) value);
                } else if ("routing".equals(entry.getKey())) {
                    getRequest.routing((String) value);
                } else if ("percolate_index".equals(entry.getKey()) || "percolate_indices".equals(entry.getKey()) || "percolateIndex".equals(entry.getKey()) || "percolateIndices".equals(entry.getKey())) {
                    if (value instanceof String[]) {
                        percolateRequest.indices((String[]) value);
                    } else {
                        percolateRequest.indices(Strings.splitStringByCommaToArray((String) value));
                    }
                } else if ("percolate_type".equals(entry.getKey()) || "percolateType".equals(entry.getKey())) {
                    percolateRequest.documentType((String) value);
                } else if ("percolate_preference".equals(entry.getKey()) || "percolatePreference".equals(entry.getKey())) {
                    percolateRequest.preference((String) value);
                } else if ("percolate_routing".equals(entry.getKey()) || "percolateRouting".equals(entry.getKey())) {
                    percolateRequest.routing((String) value);
                } else if ("ignore_indices".equals(entry.getKey()) || "ignoreIndices".equals(entry.getKey())) {
                    percolateRequest.ignoreIndices(IgnoreIndices.fromString((String) value));
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
                    if (value instanceof String[]) {
                        percolateRequest.indices((String[]) value);
                    } else {
                        percolateRequest.indices(Strings.splitStringByCommaToArray((String) value));
                    }
                } else if ("type".equals(entry.getKey())) {
                    percolateRequest.documentType((String) value);
                } else if ("preference".equals(entry.getKey())) {
                    percolateRequest.preference((String) value);
                } else if ("routing".equals(entry.getKey())) {
                    percolateRequest.routing((String) value);
                } else if ("ignore_indices".equals(entry.getKey()) || "ignoreIndices".equals(entry.getKey())) {
                    percolateRequest.ignoreIndices(IgnoreIndices.fromString((String) value));
                }
            }
        }
    }

    private String[] parseArray(XContentParser parser) throws IOException {
        final List<String> list = new ArrayList<String>();
        assert parser.currentToken() == XContentParser.Token.START_ARRAY;
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            list.add(parser.text());
        }
        return list.toArray(new String[list.size()]);
    }

    private int findNextMarker(byte marker, int from, BytesReference data, int length) {
        for (int i = from; i < length; i++) {
            if (data.get(i) == marker) {
                return i;
            }
        }
        return -1;
    }

    public List<PercolateRequest> requests() {
        return this.requests;
    }

    public IgnoreIndices ignoreIndices() {
        return ignoreIndices;
    }

    public MultiPercolateRequest ignoreIndices(IgnoreIndices ignoreIndices) {
        this.ignoreIndices = ignoreIndices;
        return this;
    }

    public String[] indices() {
        return indices;
    }

    public MultiPercolateRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public String documentType() {
        return documentType;
    }

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
        ignoreIndices = IgnoreIndices.fromId(in.readByte());
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
        out.writeByte(ignoreIndices.id());
        out.writeVInt(requests.size());
        for (PercolateRequest request : requests) {
            request.writeTo(out);
        }
    }
}
