package org.elasticsearch.action.termvector;

/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiTermVectorsRequest extends ActionRequest<MultiTermVectorsRequest> {

    String preference;
    List<TermVectorRequest> requests = new ArrayList<TermVectorRequest>();

    public MultiTermVectorsRequest add(TermVectorRequest termVectorRequest) {
        requests.add(termVectorRequest);
        return this;
    }

    public MultiTermVectorsRequest add(String index, @Nullable String type, String id) {
        requests.add(new TermVectorRequest(index, type, id));
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across
     * shards. Can be set to <tt>_local</tt> to prefer local shards,
     * <tt>_primary</tt> to execute only on primary shards, or a custom value,
     * which guarantees that the same order will be used across different
     * requests.
     */
    public MultiTermVectorsRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
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

    public void add(@Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, byte[] data, int from,
            int length) throws Exception {
        add(defaultIndex, defaultType, defaultFields, new BytesArray(data, from, length));
    }

    public void add(@Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, BytesReference data)
            throws Exception {
        XContentParser parser = XContentFactory.xContent(data).createParser(data);
        try {
            XContentParser.Token token;
            String currentFieldName = null;
            boolean offsets = true;
            boolean offsetsFound = false;
            boolean positions = true;
            boolean positionsFound = false;
            boolean payloads = true;
            boolean payloadsFound = false;
            boolean termStatistics = false;
            boolean termStatisticsFound = false;
            boolean fieldStatistics = true;
            boolean fieldStatisticsFound = false;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (currentFieldName.equals("offsets")) {
                        offsets = parser.booleanValue();
                        offsetsFound = true;
                    } else if (currentFieldName.equals("positions")) {
                        positions = parser.booleanValue();
                        positionsFound = true;
                    } else if (currentFieldName.equals("payloads")) {
                        payloads = parser.booleanValue();
                        payloadsFound = true;
                    } else if (currentFieldName.equals("term_statistics") || currentFieldName.equals("termStatistics")) {
                        termStatistics = parser.booleanValue();
                        termStatisticsFound = true;
                    } else if (currentFieldName.equals("field_statistics") || currentFieldName.equals("fieldStatistics")) {
                        fieldStatistics = parser.booleanValue();
                        fieldStatisticsFound = true;
                    } else {
                        throw new ElasticSearchParseException("_mtermvectors: Parameter " + currentFieldName + "not supported");
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {

                    if ("docs".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token != XContentParser.Token.START_OBJECT) {
                                throw new ElasticSearchIllegalArgumentException("docs array element should include an object");
                            }
                            TermVectorRequest termVectorRequest = new TermVectorRequest(defaultIndex, defaultType, null);

                            TermVectorRequest.parseRequest(termVectorRequest, parser);

                            if (defaultFields != null) {
                                termVectorRequest.selectedFields(defaultFields.clone());
                            }

                            add(termVectorRequest);
                        }
                    } else if ("ids".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (!token.isValue()) {
                                throw new ElasticSearchIllegalArgumentException("ids array element should only contain ids");
                            }
                            TermVectorRequest tvr = new TermVectorRequest(defaultIndex, defaultType, parser.text());
                            if (defaultFields != null) {
                                tvr.selectedFields(defaultFields.clone());
                            }
                            add(tvr);
                        }
                    } else {
                        throw new ElasticSearchParseException("_mtermvectors: Parameter " + currentFieldName + "not supported");
                    }
                } else if (currentFieldName != null) {
                    throw new ElasticSearchParseException("_mtermvectors: Parameter " + currentFieldName + "not supported");
                }
            }
            for (int i = 0; i < requests.size(); i++) {
                TermVectorRequest curRequest = requests.get(i);
                if (offsetsFound) {
                    curRequest.offsets(offsets);
                }
                if (payloadsFound) {
                    curRequest.payloads(payloads);
                }
                if (fieldStatisticsFound) {
                    curRequest.fieldStatistics(fieldStatistics);
                }
                if (positionsFound) {
                    curRequest.positions(positions);
                }
                if (termStatisticsFound) {
                    curRequest.termStatistics(termStatistics);
                }
                requests.set(i, curRequest);
            }
        } finally {
            parser.close();
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        preference = in.readOptionalString();
        int size = in.readVInt();
        requests = new ArrayList<TermVectorRequest>(size);
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
}
