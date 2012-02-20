package org.elasticsearch.action.search;

import com.google.common.collect.Iterators;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Iterator;

/**
 * A multi search response.
 */
public class MultiSearchResponse implements ActionResponse, Iterable<MultiSearchResponse.Item>, ToXContent {

    /**
     * A search response item, holding the actual search response, or an error message if it failed.
     */
    public static class Item implements Streamable {
        private SearchResponse response;
        private String failureMessage;

        Item() {

        }

        public Item(SearchResponse response, String failureMessage) {
            this.response = response;
            this.failureMessage = failureMessage;
        }

        /**
         * Is it a failed search?
         */
        public boolean isFailure() {
            return failureMessage != null;
        }

        /**
         * The actual failure message, null if its not a failure.
         */
        @Nullable
        public String failureMessage() {
            return failureMessage;
        }

        /**
         * The actual failure message, null if its not a failure.
         */
        @Nullable
        public String getFailureMessage() {
            return failureMessage;
        }

        /**
         * The actual search response, null if its a failure.
         */
        @Nullable
        public SearchResponse response() {
            return this.response;
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
                failureMessage = in.readUTF();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (response != null) {
                out.writeBoolean(true);
                response.writeTo(out);
            } else {
                out.writeUTF(failureMessage);
            }
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
        return Iterators.forArray(items);
    }

    /**
     * The list of responses, the order is the same as the one provided in the request.
     */
    public Item[] responses() {
        return this.items;
    }

    /**
     * The list of responses, the order is the same as the one provided in the request.
     */
    public Item[] getResponses() {
        return this.items;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        items = new Item[in.readVInt()];
        for (int i = 0; i < items.length; i++) {
            items[i] = Item.readItem(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(items.length);
        for (Item item : items) {
            item.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.RESPONSES);
        for (Item item : items) {
            if (item.isFailure()) {
                builder.startObject();
                builder.field(Fields.ERROR, item.failureMessage());
                builder.endObject();
            } else {
                builder.startObject();
                item.response().toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endArray();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString RESPONSES = new XContentBuilderString("responses");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
    }
}
