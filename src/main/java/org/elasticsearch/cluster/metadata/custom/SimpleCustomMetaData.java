package org.elasticsearch.cluster.metadata.custom;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData.Custom;
import org.elasticsearch.cluster.metadata.IndexMetaData.Custom.Factory;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/** 
 * This simple custom meta data class only supports simple name value pairs.
 * It doesn't support nested name value pairs.
 *  {
 *    ....
 *      ,
 *      "custom_meta" : {
 *          "name1" : "value1",
 *          "name2" : { "attr1": "value1", "attr2":"value2"},
 *          "name3" : number
 *       }
 *  }
 */
public class SimpleCustomMetaData implements IndexMetaData.Custom {

    public static final String TYPE = "custom_meta";

    public static final Factory FACTORY = new Factory();

    static {
        IndexMetaData.registerFactory(TYPE, FACTORY);
    }

    public static class Entry {
        private final String name;
        private final BytesReference source;

        public Entry(String name, BytesReference source) {
            this.name = name;
            this.source = source;
        }

        public String name() {
            return this.name;
        }

        @Nullable
        public BytesReference source() {
            return this.source;
        }
    }

    private final ImmutableList<Entry> entries;


    public SimpleCustomMetaData(Entry... entries) {
        this.entries = ImmutableList.copyOf(entries);
    }

    public ImmutableList<Entry> entries() {
        return this.entries;
    }


    @Override
    public String type() {
        return TYPE;
    }

    public static class Factory implements IndexMetaData.Custom.Factory<SimpleCustomMetaData> {

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public SimpleCustomMetaData readFrom(StreamInput in) throws IOException {
            Entry[] entries = new Entry[in.readVInt()];
            for (int i = 0; i < entries.length; i++) {
                entries[i] = new Entry(in.readUTF(), in.readBoolean() ? in.readBytesReference() : null);
            }
            return new SimpleCustomMetaData(entries);
        }

        @Override
        public void writeTo(SimpleCustomMetaData metaData, StreamOutput out) throws IOException {
            out.writeVInt(metaData.entries.size());
            for (Entry entry: metaData.entries) {
                out.writeUTF(entry.name);
                if (entry.source() == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeBytesReference(entry.source());
                }
            }
        }

        @Override
        public SimpleCustomMetaData fromMap(Map<String, Object> map) throws IOException {

            // if it starts with the type, remove it
            if (map.size() == 1 && map.containsKey(TYPE)) {
                map = (Map<String, Object>) map.values().iterator().next();
            }
            
            XContentBuilder builder = XContentFactory.smileBuilder().map(map);
            XContentParser parser = XContentFactory.xContent(XContentType.SMILE).createParser(builder.bytes());
            try {
                // move to START_OBJECT
                parser.nextToken();
                return fromXContent(parser);
            } finally {
                parser.close();
            }
        }

        @Override
        public SimpleCustomMetaData fromXContent(XContentParser parser) throws IOException {
            // we get here after we are at custom meta token
            XContentParser.Token token;
            List<Entry> entries = new ArrayList<Entry>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                String name = null;
                BytesReference source = null;
                if (token == XContentParser.Token.FIELD_NAME) {
                    name = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    XContentBuilder builder = XContentFactory.jsonBuilder().map(parser.mapOrdered());
                    source = builder.bytes();
                } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                    source = new BytesArray(parser.binaryValue());
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    source = new BytesArray(parser.text().getBytes());
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    source = new BytesArray(String.valueOf(parser.booleanValue()).getBytes());
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    source = new BytesArray(String.valueOf(parser.numberValue()).getBytes());
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    source = new BytesArray("".getBytes());
                }
                if (name != null)
                    entries.add(new Entry(name,source));
            }
            return new SimpleCustomMetaData(entries.toArray(new Entry[entries.size()]));
        }

        @Override
        public void toXContent(SimpleCustomMetaData customMeta, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(TYPE, XContentBuilder.FieldCaseConversion.NONE);
            for (Entry entry : customMeta.entries()) {
                toXContent(entry, builder, params);
            }
            builder.endObject();
        }

        public void toXContent(Entry entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
            boolean binary = params.paramAsBoolean("binary", false);
            builder.field(entry.name);
            if (binary) {
                builder.value(entry.source());
            } else {
                Map<String, Object> mapping = XContentFactory.xContent(entry.source()).createParser(entry.source()).mapOrderedAndClose();
                builder.map(mapping);
            }
        }

        @Override
        public SimpleCustomMetaData merge(SimpleCustomMetaData first, SimpleCustomMetaData second) {
            List<Entry> entries = Lists.newArrayList();
            entries.addAll(first.entries());
            for (Entry secondEntry : second.entries()) {
                boolean found = false;
                for (Entry firstEntry : first.entries()) {
                    if (firstEntry.name().equals(secondEntry.name())) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    entries.add(secondEntry);
                }
            }
            return new SimpleCustomMetaData(entries.toArray(new Entry[entries.size()]));
        }
    }

}
