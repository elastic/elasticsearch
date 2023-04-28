/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A multi search response.
 */
public class MultiSearchResponse extends ActionResponse implements Iterable<MultiSearchResponse.Item>, ChunkedToXContentObject {

    private static final ParseField RESPONSES = new ParseField(Fields.RESPONSES);
    private static final ParseField TOOK_IN_MILLIS = new ParseField("took");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<MultiSearchResponse, Void> PARSER = new ConstructingObjectParser<>(
        "multi_search",
        true,
        a -> new MultiSearchResponse(((List<Item>) a[0]).toArray(new Item[0]), (long) a[1])
    );
    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> itemFromXContent(p), RESPONSES);
        PARSER.declareLong(constructorArg(), TOOK_IN_MILLIS);
    }

    /**
     * A search response item, holding the actual search response, or an error message if it failed.
     */
    public static class Item implements Writeable, ChunkedToXContent {
        private final SearchResponse response;
        private final Exception exception;

        public Item(SearchResponse response, Exception exception) {
            this.response = response;
            this.exception = exception;
        }

        Item(StreamInput in) throws IOException {
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

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            if (isFailure()) {
                return Iterators.concat(
                    ChunkedToXContentHelper.startObject(),
                    Iterators.single((b, p) -> ElasticsearchException.generateFailureXContent(b, p, Item.this.getFailure(), true)),
                    Iterators.single((b, p) -> b.field(Fields.STATUS, ExceptionsHelper.status(Item.this.getFailure()).getStatus())),
                    ChunkedToXContentHelper.endObject()
                );
            } else {
                return Iterators.concat(
                    ChunkedToXContentHelper.startObject(),
                    Item.this.getResponse().innerToXContentChunked(params),
                    Iterators.single((b, p) -> b.field(Fields.STATUS, Item.this.getResponse().status().getStatus())),
                    ChunkedToXContentHelper.endObject()
                );
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
        items = in.readArray(Item::new, Item[]::new);
        tookInMillis = in.readVLong();
    }

    public MultiSearchResponse(Item[] items, long tookInMillis) {
        this.items = items;
        this.tookInMillis = tookInMillis;
    }

    @Override
    public Iterator<Item> iterator() {
        return Iterators.forArray(items);
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
        out.writeArray(items);
        out.writeVLong(tookInMillis);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            Iterators.single((b, p) -> b.field("took", tookInMillis).startArray(Fields.RESPONSES)),
            Iterators.flatMap(Iterators.forArray(items), item -> item.toXContentChunked(params)),
            Iterators.single((b, p) -> b.endArray()),
            ChunkedToXContentHelper.endObject()
        );
    }

    public static MultiSearchResponse fromXContext(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static MultiSearchResponse.Item itemFromXContent(XContentParser parser) throws IOException {
        // This parsing logic is a bit tricky here, because the multi search response itself is tricky:
        // 1) The json objects inside the responses array are either a search response or a serialized exception
        // 2) Each response json object gets a status field injected that ElasticsearchException.failureFromXContent(...) does not parse,
        // but SearchResponse.innerFromXContent(...) parses and then ignores. The status field is not needed to parse
        // the response item. However in both cases this method does need to parse the 'status' field otherwise the parsing of
        // the response item in the next json array element will fail due to parsing errors.

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
