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

package org.elasticsearch.action.termvectors;

import com.google.common.collect.Iterators;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;

public class MultiTermVectorsRequest extends ActionRequest<MultiTermVectorsRequest> implements Iterable<TermVectorsRequest>, CompositeIndicesRequest, RealtimeRequest {

    String preference;
    List<TermVectorsRequest> requests = new ArrayList<>();

    final Set<String> ids = new HashSet<>();

    public MultiTermVectorsRequest add(TermVectorsRequest termVectorsRequest) {
        requests.add(termVectorsRequest);
        return this;
    }

    public MultiTermVectorsRequest add(String index, @Nullable String type, String id) {
        requests.add(new TermVectorsRequest(index, type, id));
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
                    validationException = ValidateActions.addValidationError("at multi term vectors for doc " + i,
                            validationExceptionForDoc);
                }
            }
        }
        return validationException;
    }

    @Override
    public List<? extends IndicesRequest> subRequests() {
        return requests;
    }

    @Override
    public Iterator<TermVectorsRequest> iterator() {
        return Iterators.unmodifiableIterator(requests.iterator());
    }

    public boolean isEmpty() {
        return requests.isEmpty() && ids.isEmpty();
    }

    public List<TermVectorsRequest> getRequests() {
        return requests;
    }

    public void add(TermVectorsRequest template, BytesReference data) throws Exception {
        XContentParser.Token token;
        String currentFieldName = null;
        if (data.length() > 0) {
            try (XContentParser parser = XContentFactory.xContent(data).createParser(data)) {
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
                                TermVectorsRequest.parseRequest(termVectorsRequest, parser);
                                add(termVectorsRequest);
                            }
                        } else if ("ids".equals(currentFieldName)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (!token.isValue()) {
                                    throw new IllegalArgumentException("ids array element should only contain ids");
                                }
                                ids.add(parser.text());
                            }
                        } else {
                            throw new ElasticsearchParseException("no parameter named [{}] and type ARRAY", currentFieldName);
                        }
                    } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                        if ("parameters".equals(currentFieldName)) {
                            TermVectorsRequest.parseRequest(template, parser);
                        } else {
                            throw new ElasticsearchParseException("no parameter named [{}] and type OBJECT", currentFieldName);
                        }
                    } else if (currentFieldName != null) {
                        throw new ElasticsearchParseException("_mtermvectors: Parameter [{}] not supported", currentFieldName);
                    }
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        preference = in.readOptionalString();
        int size = in.readVInt();
        requests = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            requests.add(TermVectorsRequest.readTermVectorsRequest(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeVInt(requests.size());
        for (TermVectorsRequest termVectorsRequest : requests) {
            termVectorsRequest.writeTo(out);
        }
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
    public MultiTermVectorsRequest realtime(Boolean realtime) {
        for (TermVectorsRequest request : requests) {
            request.realtime(realtime);
        }
        return this;
    }
}
