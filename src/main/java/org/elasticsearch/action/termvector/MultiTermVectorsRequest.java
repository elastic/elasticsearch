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

package org.elasticsearch.action.termvector;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MultiTermVectorsRequest extends ActionRequest<MultiTermVectorsRequest> {

    String preference;
    List<TermVectorRequest> requests = new ArrayList<>();

    final Set<String> ids = new HashSet<>();

    public MultiTermVectorsRequest add(TermVectorRequest termVectorRequest) {
        requests.add(termVectorRequest);
        return this;
    }

    public MultiTermVectorsRequest add(String index, @Nullable String type, String id) {
        requests.add(new TermVectorRequest(index, type, id));
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = ValidateActions.addValidationError("multi term vectors: no documents requested", validationException);
        } else {
            for (int i = 0; i < requests.size(); i++) {
                TermVectorRequest termVectorRequest = requests.get(i);
                ActionRequestValidationException validationExceptionForDoc = termVectorRequest.validate();
                if (validationExceptionForDoc != null) {
                    validationException = ValidateActions.addValidationError("at multi term vectors for doc " + i,
                            validationExceptionForDoc);
                }
            }
        }
        return validationException;
    }

    public void add(TermVectorRequest template, BytesReference data)
            throws Exception {

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
                                    throw new ElasticsearchIllegalArgumentException("docs array element should include an object");
                                }
                                TermVectorRequest termVectorRequest = new TermVectorRequest(template);
                                TermVectorRequest.parseRequest(termVectorRequest, parser);
                                add(termVectorRequest);
                            }
                        } else if ("ids".equals(currentFieldName)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (!token.isValue()) {
                                    throw new ElasticsearchIllegalArgumentException("ids array element should only contain ids");
                                }
                                ids.add(parser.text());
                            }
                        } else {
                            throw new ElasticsearchParseException(
                                    "No parameter named " + currentFieldName + "and type ARRAY");
                        }
                    } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                        if ("parameters".equals(currentFieldName)) {
                            TermVectorRequest.parseRequest(template, parser);
                        } else {
                            throw new ElasticsearchParseException(
                                    "No parameter named " + currentFieldName + "and type OBJECT");
                        }
                    } else if (currentFieldName != null) {
                        throw new ElasticsearchParseException("_mtermvectors: Parameter " + currentFieldName + "not supported");
                    }
                }
            }
        }
        for (String id : ids) {
            TermVectorRequest curRequest = new TermVectorRequest(template);
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
            requests.add(TermVectorRequest.readTermVectorRequest(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeVInt(requests.size());
        for (TermVectorRequest termVectorRequest : requests) {
            termVectorRequest.writeTo(out);
        }
    }

    public void ids(String[] ids) {
        for (String id : ids) {
            this.ids.add(id.replaceAll("\\s", ""));
        }
    }
}
