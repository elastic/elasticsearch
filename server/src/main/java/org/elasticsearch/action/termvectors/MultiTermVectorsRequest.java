/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

// It's not possible to suppress teh warning at #realtime(boolean) at a method-level.
@SuppressWarnings("unchecked")
public class MultiTermVectorsRequest extends LegacyActionRequest
    implements
        Iterable<TermVectorsRequest>,
        CompositeIndicesRequest,
        RealtimeRequest {

    String preference;
    List<TermVectorsRequest> requests = new ArrayList<>();

    final Set<String> ids = new HashSet<>();

    public MultiTermVectorsRequest(StreamInput in) throws IOException {
        super(in);
        preference = in.readOptionalString();
        int size = in.readVInt();
        requests = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            requests.add(new TermVectorsRequest(in));
        }
    }

    public MultiTermVectorsRequest() {}

    public MultiTermVectorsRequest add(TermVectorsRequest termVectorsRequest) {
        requests.add(termVectorsRequest);
        return this;
    }

    public MultiTermVectorsRequest add(String index, String id) {
        requests.add(new TermVectorsRequest(index, id));
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = ValidateActions.addValidationError("multi term vectors: no documents requested", validationException);
        } else {
            for (int i = 0; i < requests.size(); i++) {
                TermVectorsRequest termVectorsRequest = requests.get(i);
                ActionRequestValidationException validationExceptionForDoc = termVectorsRequest.validate();
                if (validationExceptionForDoc != null) {
                    validationException = ValidateActions.addValidationError(
                        "at multi term vectors for doc " + i,
                        validationExceptionForDoc
                    );
                }
            }
        }
        return validationException;
    }

    @Override
    public Iterator<TermVectorsRequest> iterator() {
        return Collections.unmodifiableCollection(requests).iterator();
    }

    public boolean isEmpty() {
        return requests.isEmpty() && ids.isEmpty();
    }

    public List<TermVectorsRequest> getRequests() {
        return requests;
    }

    public void add(TermVectorsRequest template, @Nullable XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        if (parser != null) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("docs".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token != XContentParser.Token.START_OBJECT) {
                                throw new IllegalArgumentException("docs array element should include an object");
                            }
                            TermVectorsRequest termVectorsRequest = new TermVectorsRequest(template);
                            TermVectorsRequest.parseRequest(termVectorsRequest, parser, parser.getRestApiVersion());
                            add(termVectorsRequest);
                        }
                    } else if ("ids".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new IllegalArgumentException("ids array element should only contain ids");
                            }
                            ids.add(parser.text());
                        }
                    } else {
                        throw new ElasticsearchParseException("no parameter named [{}] and type ARRAY", currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    if ("parameters".equals(currentFieldName)) {
                        TermVectorsRequest.parseRequest(template, parser, parser.getRestApiVersion());
                    } else {
                        throw new ElasticsearchParseException("no parameter named [{}] and type OBJECT", currentFieldName);
                    }
                } else if (currentFieldName != null) {
                    throw new ElasticsearchParseException("_mtermvectors: Parameter [{}] not supported", currentFieldName);
                }
            }
        }
        for (String id : ids) {
            TermVectorsRequest curRequest = new TermVectorsRequest(template);
            curRequest.id(id);
            requests.add(curRequest);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeCollection(requests);
    }

    public void ids(String[] ids) {
        for (String id : ids) {
            this.ids.add(id.replaceAll("\\s", ""));
        }
    }

    public int size() {
        return requests.size();
    }

    @Override
    @SuppressWarnings("unchecked")
    public MultiTermVectorsRequest realtime(boolean realtime) {
        for (TermVectorsRequest request : requests) {
            request.realtime(realtime);
        }
        return this;
    }
}
