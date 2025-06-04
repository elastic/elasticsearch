/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
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
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;

/**
 * A multi search response.
 */
public class MultiSearchResponse extends ActionResponse implements Iterable<MultiSearchResponse.Item>, ChunkedToXContentObject {

    /**
     * A search response item, holding the actual search response, or an error message if it failed.
     */
    public static class Item implements Writeable, ChunkedToXContent {
        private final SearchResponse response;
        private final Exception exception;

        /**
         *
         * @param response search response that is considered owned by this instance after this constructor returns or {@code null}
         * @param exception exception in case of search failure
         */
        public Item(@Nullable SearchResponse response, @Nullable Exception exception) {
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

    private final RefCounted refCounted = LeakTracker.wrap(new SimpleRefCounted());

    public MultiSearchResponse(StreamInput in) throws IOException {
        items = in.readArray(Item::new, Item[]::new);
        tookInMillis = in.readVLong();
    }

    /**
     * @param items individual search responses, the elements in this array are considered as owned by this instance for ref-counting
     *              purposes if their {@link Item#response} is non-null
     */
    public MultiSearchResponse(Item[] items, long tookInMillis) {
        this.items = items;
        this.tookInMillis = tookInMillis;
    }

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        if (refCounted.decRef()) {
            deallocate();
            return true;
        }
        return false;
    }

    private void deallocate() {
        for (int i = 0; i < items.length; i++) {
            Item item = items[i];
            var r = item.response;
            if (r != null) {
                r.decRef();
                items[i] = null;
            }
        }
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
    }

    @Override
    public Iterator<Item> iterator() {
        assert hasReferences();
        return Iterators.forArray(items);
    }

    /**
     * The list of responses, the order is the same as the one provided in the request.
     */
    public Item[] getResponses() {
        assert hasReferences();
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
        assert hasReferences();
        out.writeArray(items);
        out.writeVLong(tookInMillis);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        assert hasReferences();
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            Iterators.single((b, p) -> b.field("took", tookInMillis).startArray(Fields.RESPONSES)),
            Iterators.flatMap(Iterators.forArray(items), item -> item.toXContentChunked(params)),
            Iterators.single((b, p) -> b.endArray()),
            ChunkedToXContentHelper.endObject()
        );
    }

    public static final class Fields {
        public static final String RESPONSES = "responses";
        static final String STATUS = "status";
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
