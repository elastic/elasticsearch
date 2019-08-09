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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A multi search response.
 */
public class MultiSearchResponse extends ActionResponse implements Iterable<MultiSearchResponse.Item>, ToXContentObject {

    private static final ParseField RESPONSES = new ParseField(Fields.RESPONSES);
    private static final ParseField TOOK_IN_MILLIS = new ParseField("took");
    private static final ConstructingObjectParser<MultiSearchResponse, Void> PARSER = new ConstructingObjectParser<>("multi_search",
            true, a -> new MultiSearchResponse(((List<Item>)a[0]).toArray(new Item[0]), (long) a[1]));
    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> itemFromXContent(p), RESPONSES);
        PARSER.declareLong(constructorArg(), TOOK_IN_MILLIS);
    }

    /**
     * A search response item, holding the actual search response, or an error message if it failed.
     */
    public static class Item implements Writeable {
        private final SearchResponse response;
        private final Exception exception;

        public Item(SearchResponse response, Exception exception) {
            this.response = response;
            this.exception = exception;
        }

        Item(StreamInput in) throws IOException{
            if (in.readBoolean()) {
                this.response = new SearchResponse(in);
                this.exception = null;
            } else {
                this.exception = in.readException();
                this.response = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (response != null) {
                out.writeBoolean(true);
                response.writeTo(out);
            } else {
                out.writeBoolean(false);
                out.writeException(exception);
            }
        }

        /**
         * Is it a failed search?
         */
        public boolean isFailure() {
            return exception != null;
        }

        /**
         * The actual failure message, null if its not a failure.
         */
        @Nullable
        public String getFailureMessage() {
            return exception == null ? null : exception.getMessage();
        }

        /**
         * The actual search response, null if its a failure.
         */
        @Nullable
        public SearchResponse getResponse() {
            return this.response;
        }

        public Exception getFailure() {
            return exception;
        }
    }

    private final Item[] items;
    private final long tookInMillis;

    public MultiSearchResponse(StreamInput in) throws IOException {
        super(in);
        items = new Item[in.readVInt()];
        for (int i = 0; i < items.length; i++) {
            items[i] = new Item(in);
        }
        tookInMillis = in.readVLong();
    }

    public MultiSearchResponse(Item[] items, long tookInMillis) {
        this.items = items;
        this.tookInMillis = tookInMillis;
    }

    @Override
    public Iterator<Item> iterator() {
        return Arrays.stream(items).iterator();
    }

    /**
     * The list of responses, the order is the same as the one provided in the request.
     */
    public Item[] getResponses() {
        return this.items;
    }

    /**
     * How long the msearch took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(items.length);
        for (Item item : items) {
            item.writeTo(out);
        }
        out.writeVLong(tookInMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("took", tookInMillis);
        builder.startArray(Fields.RESPONSES);
        for (Item item : items) {
            builder.startObject();
            if (item.isFailure()) {
                ElasticsearchException.generateFailureXContent(builder, params, item.getFailure(), true);
                builder.field(Fields.STATUS, ExceptionsHelper.status(item.getFailure()).getStatus());
            } else {
                item.getResponse().innerToXContent(builder, params);
                builder.field(Fields.STATUS, item.getResponse().status().getStatus());
            }
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static MultiSearchResponse fromXContext(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static MultiSearchResponse.Item itemFromXContent(XContentParser parser) throws IOException {
        // This parsing logic is a bit tricky here, because the multi search response itself is tricky:
        // 1) The json objects inside the responses array are either a search response or a serialized exception
        // 2) Each response json object gets a status field injected that ElasticsearchException.failureFromXContent(...) does not parse,
        //    but SearchResponse.innerFromXContent(...) parses and then ignores. The status field is not needed to parse
        //    the response item. However in both cases this method does need to parse the 'status' field otherwise the parsing of
        //    the response item in the next json array element will fail due to parsing errors.

        Item item = null;
        String fieldName = null;

        Token token = parser.nextToken();
        assert token == Token.FIELD_NAME;
        outer: for (; token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    fieldName = parser.currentName();
                    if ("error".equals(fieldName)) {
                        item = new Item(null, ElasticsearchException.failureFromXContent(parser));
                    } else if ("status".equals(fieldName) == false) {
                        item = new Item(SearchResponse.innerFromXContent(parser), null);
                        break outer;
                    }
                    break;
                case VALUE_NUMBER:
                    if ("status".equals(fieldName)) {
                        // Ignore the status value
                    }
                    break;
            }
        }
        assert parser.currentToken() == Token.END_OBJECT;
        return item;
    }

    static final class Fields {
        static final String RESPONSES = "responses";
        static final String STATUS = "status";
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
