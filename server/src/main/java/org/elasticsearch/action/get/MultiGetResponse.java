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

package org.elasticsearch.action.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MultiGetResponse extends ActionResponse implements Iterable<MultiGetItemResponse>, ToXContentObject {

    private static final ParseField INDEX = new ParseField("_index");
    private static final ParseField ID = new ParseField("_id");
    private static final ParseField ERROR = new ParseField("error");
    private static final ParseField DOCS = new ParseField("docs");

    /**
     * Represents a failure.
     */
    public static class Failure implements Writeable, ToXContentObject {

        private final String index;
        private final String id;
        private final Exception exception;

        public Failure(String index, String id, Exception exception) {
            this.index = index;
            this.id = id;
            this.exception = exception;
        }

        Failure(StreamInput in) throws IOException {
            index = in.readString();
            if (in.getVersion().before(Version.V_8_0_0)) {
                in.readOptionalString();
            }
            id = in.readString();
            exception = in.readException();
        }

        /**
         * The index name of the action.
         */
        public String getIndex() {
            return this.index;
        }

        /**
         * The id of the action.
         */
        public String getId() {
            return id;
        }

        /**
         * The failure message.
         */
        public String getMessage() {
            return exception != null ? exception.getMessage() : null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            if (out.getVersion().before(Version.V_8_0_0)) {
                out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeString(id);
            out.writeException(exception);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX.getPreferredName(), index);
            builder.field(ID.getPreferredName(), id);
            ElasticsearchException.generateFailureXContent(builder, params, exception, true);
            builder.endObject();
            return builder;
        }

        public Exception getFailure() {
            return exception;
        }
    }

    private final MultiGetItemResponse[] responses;

    public MultiGetResponse(MultiGetItemResponse[] responses) {
        this.responses = responses;
    }

    MultiGetResponse(StreamInput in) throws IOException {
        super(in);
        responses = new MultiGetItemResponse[in.readVInt()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = new MultiGetItemResponse(in);
        }
    }

    public MultiGetItemResponse[] getResponses() {
        return this.responses;
    }

    @Override
    public Iterator<MultiGetItemResponse> iterator() {
        return Arrays.stream(responses).iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(DOCS.getPreferredName());
        for (MultiGetItemResponse response : responses) {
            if (response.isFailed()) {
                Failure failure = response.getFailure();
                failure.toXContent(builder, params);
            } else {
                GetResponse getResponse = response.getResponse();
                getResponse.toXContent(builder, params);
            }
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static MultiGetResponse fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        List<MultiGetItemResponse> items = new ArrayList<>();
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    currentFieldName = parser.currentName();
                    break;
                case START_ARRAY:
                    if (DOCS.getPreferredName().equals(currentFieldName)) {
                        for (token = parser.nextToken(); token != Token.END_ARRAY; token = parser.nextToken()) {
                            if (token == Token.START_OBJECT) {
                                items.add(parseItem(parser));
                            }
                        }
                    }
                    break;
                default:
                    // If unknown tokens are encounter then these should be ignored, because
                    // this is parsing logic on the client side.
                    break;
            }
        }
        return new MultiGetResponse(items.toArray(new MultiGetItemResponse[0]));
    }

    private static MultiGetItemResponse parseItem(XContentParser parser) throws IOException {
        String currentFieldName = null;
        String index = null;
        String id = null;
        ElasticsearchException exception = null;
        GetResult getResult = null;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    currentFieldName = parser.currentName();
                    if (INDEX.match(currentFieldName, parser.getDeprecationHandler()) == false
                            && ID.match(currentFieldName, parser.getDeprecationHandler()) == false
                            && ERROR.match(currentFieldName, parser.getDeprecationHandler()) == false) {
                        getResult = GetResult.fromXContentEmbedded(parser, index, id);
                    }
                    break;
                case VALUE_STRING:
                    if (INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                        index = parser.text();
                    } else if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                        id = parser.text();
                    }
                    break;
                case START_OBJECT:
                    if (ERROR.match(currentFieldName, parser.getDeprecationHandler())) {
                        exception = ElasticsearchException.fromXContent(parser);
                    }
                    break;
                default:
                    // If unknown tokens are encounter then these should be ignored, because
                    // this is parsing logic on the client side.
                    break;
            }
            if (getResult != null) {
                break;
            }
        }

        if (exception != null) {
            return new MultiGetItemResponse(null, new Failure(index, id, exception));
        } else {
            GetResponse getResponse = new GetResponse(getResult);
            return new MultiGetItemResponse(getResponse, null);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(responses.length);
        for (MultiGetItemResponse response : responses) {
            response.writeTo(out);
        }
    }
}
