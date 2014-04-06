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
package org.elasticsearch.action.percolate;

import com.google.common.collect.Iterators;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Iterator;

/**
 */
public class MultiPercolateResponse extends ActionResponse implements Iterable<MultiPercolateResponse.Item>, ToXContent {

    private Item[] items;

    public MultiPercolateResponse(Item[] items) {
        this.items = items;
    }

    public MultiPercolateResponse() {
        this.items = new Item[0];
    }

    @Override
    public Iterator<Item> iterator() {
        return Iterators.forArray(items);
    }

    public Item[] items() {
        return items;
    }

    public Item[] getItems() {
        return items;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.RESPONSES);
        for (MultiPercolateResponse.Item item : items) {
            if (item.isFailure()) {
                builder.startObject();
                builder.field(Fields.ERROR, item.getErrorMessage());
                builder.endObject();
            } else {
                builder.startObject();
                item.getResponse().toXContent(builder, params);
                builder.endObject();
            }
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

    public static class Item implements Streamable {

        private PercolateResponse response;
        private String errorMessage;

        public Item(PercolateResponse response) {
            this.response = response;
        }

        public Item(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public Item() {
        }

        public PercolateResponse response() {
            return response;
        }

        public String errorMessage() {
            return errorMessage;
        }

        public PercolateResponse getResponse() {
            return response;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public boolean isFailure() {
            return errorMessage != null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                response = new PercolateResponse();
                response.readFrom(in);
            } else {
                errorMessage = in.readString();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (response != null) {
                out.writeBoolean(true);
                response.writeTo(out);
            } else {
                out.writeBoolean(false);
                out.writeString(errorMessage);
            }
        }

    }

    static final class Fields {
        static final XContentBuilderString RESPONSES = new XContentBuilderString("responses");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
    }

}
