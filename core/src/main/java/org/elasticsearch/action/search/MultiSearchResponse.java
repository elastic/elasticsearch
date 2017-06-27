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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A multi search response.
 */
public class MultiSearchResponse extends ActionResponse implements Iterable<MultiSearchResponse.Item>, ToXContentObject {

    /**
     * A search response item, holding the actual search response, or an error message if it failed.
     */
    public static class Item implements Streamable {
        private SearchResponse response;
        private Exception exception;

        Item() {

        }

        public Item(SearchResponse response, Exception exception) {
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
        public SearchResponse getResponse() {
            return this.response;
        }

        public static Item readItem(StreamInput in) throws IOException {
            Item item = new Item();
            item.readFrom(in);
            return item;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                this.response = new SearchResponse();
                response.readFrom(in);
            } else {
                exception = in.readException();
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

        public Exception getFailure() {
            return exception;
        }
    }

    private Item[] items;

    MultiSearchResponse() {
    }

    public MultiSearchResponse(Item[] items) {
        this.items = items;
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        items = new Item[in.readVInt()];
        for (int i = 0; i < items.length; i++) {
            items[i] = Item.readItem(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(items.length);
        for (Item item : items) {
            item.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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

    static final class Fields {
        static final String RESPONSES = "responses";
        static final String STATUS = "status";
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
