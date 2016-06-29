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
package org.elasticsearch.percolator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Represents the response of a multi percolate request.
 *
 * Each item represents the response of a percolator request and the order of the items is in the same order as the
 * percolator requests were defined in the multi percolate request.
 *
 * @deprecated Instead use multi search API with {@link PercolateQueryBuilder}
 */
@Deprecated
public class MultiPercolateResponse extends ActionResponse implements Iterable<MultiPercolateResponse.Item>, ToXContent {

    private Item[] items;

    MultiPercolateResponse(Item[] items) {
        this.items = items;
    }

    MultiPercolateResponse() {
        this.items = new Item[0];
    }

    @Override
    public Iterator<Item> iterator() {
        return Arrays.stream(items).iterator();
    }

    /**
     * Same as {@link #getItems()}
     */
    public Item[] items() {
        return items;
    }

    /**
     * @return the percolate responses as items.
     */
    public Item[] getItems() {
        return items;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.RESPONSES);
        for (MultiPercolateResponse.Item item : items) {
            builder.startObject();
            if (item.isFailure()) {
                ElasticsearchException.renderThrowable(builder, params, item.getFailure());
            } else {
                item.getResponse().toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        items = new Item[size];
        for (int i = 0; i < items.length; i++) {
            items[i] = new Item();
            items[i].readFrom(in);
        }
    }

    /**
     * Encapsulates a single percolator response which may contain an error or the actual percolator response itself.
     */
    public static class Item implements Streamable {

        private PercolateResponse response;
        private Throwable throwable;

        Item(PercolateResponse response) {
            this.response = response;
        }

        Item(Throwable Throwable) {
            this.throwable = Throwable;
        }

        Item() {
        }


        /**
         * @return The percolator response or <code>null</code> if there was error.
         */
        @Nullable
        public PercolateResponse getResponse() {
            return response;
        }

        /**
         * @return An error description if there was an error or <code>null</code> if the percolate request was successful
         */
        @Nullable
        public String getErrorMessage() {
            return throwable == null ? null : throwable.getMessage();
        }

        /**
         * @return <code>true</code> if the percolator request that this item represents failed otherwise
         * <code>false</code> is returned.
         */
        public boolean isFailure() {
            return throwable != null;
        }

        public Throwable getFailure() {
            return throwable;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                response = new PercolateResponse();
                response.readFrom(in);
            } else {
                throwable = in.readThrowable();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (response != null) {
                out.writeBoolean(true);
                response.writeTo(out);
            } else {
                out.writeBoolean(false);
                out.writeThrowable(throwable);
            }
        }
    }

    static final class Fields {
        static final String RESPONSES = "responses";
        static final String ERROR = "error";
    }

}
