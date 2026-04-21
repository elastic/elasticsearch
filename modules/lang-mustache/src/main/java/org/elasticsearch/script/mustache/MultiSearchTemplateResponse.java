/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;

public class MultiSearchTemplateResponse extends ActionResponse
    implements
        Iterable<MultiSearchTemplateResponse.Item>,
        ChunkedToXContentObject {

    /**
     * A search template response item, holding the actual search template response, or an error message if it failed.
     */
    public static class Item implements Writeable, ChunkedToXContent {
        private final SearchTemplateResponse response;
        private final Exception exception;

        public Item(SearchTemplateResponse response, Exception exception) {
            this.response = response;
            this.exception = exception;
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
        public SearchTemplateResponse getResponse() {
            return this.response;
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
                    Iterators.single((b, p) -> ElasticsearchException.generateFailureXContent(b, p, getFailure(), true)),
                    Iterators.single((b, p) -> b.field(Fields.STATUS, ExceptionsHelper.status(getFailure()).getStatus())),
                    ChunkedToXContentHelper.endObject()
                );
            } else {
                return Iterators.concat(
                    ChunkedToXContentHelper.startObject(),
                    getResponse().innerToXContentChunked(params),
                    Iterators.single((b, p) -> b.field(Fields.STATUS, getResponse().status().getStatus())),
                    ChunkedToXContentHelper.endObject()
                );
            }
        }

        public Exception getFailure() {
            return exception;
        }

        @Override
        public String toString() {
            return "Item [response=" + response + ", exception=" + exception + "]";
        }
    }

    private final Item[] items;
    private final long tookInMillis;

    private final RefCounted refCounted = LeakTracker.wrap(new AbstractRefCounted() {
        @Override
        protected void closeInternal() {
            for (int i = 0; i < items.length; i++) {
                Item item = items[i];
                var r = item.response;
                if (r != null) {
                    r.decRef();
                    items[i] = null;
                }
            }
        }
    });

    MultiSearchTemplateResponse(Item[] items, long tookInMillis) {
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
     * How long the msearch_template took.
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
        return refCounted.decRef();
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
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
