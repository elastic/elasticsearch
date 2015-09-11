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

package org.elasticsearch.search.warmer;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class IndexWarmersMetaData extends AbstractDiffable<IndexMetaData.Custom> implements IndexMetaData.Custom {

    public static final String TYPE = "warmers";

    public static final IndexWarmersMetaData PROTO = new IndexWarmersMetaData();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexWarmersMetaData that = (IndexWarmersMetaData) o;

        return entries.equals(that.entries);

    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    public static class Entry {
        private final String name;
        private final String[] types;
        private final BytesReference source;
        private final Boolean requestCache;

        public Entry(String name, String[] types, Boolean requestCache, BytesReference source) {
            this.name = name;
            this.types = types == null ? Strings.EMPTY_ARRAY : types;
            this.source = source;
            this.requestCache = requestCache;
        }

        public String name() {
            return this.name;
        }

        public String[] types() {
            return this.types;
        }

        @Nullable
        public BytesReference source() {
            return this.source;
        }

        @Nullable
        public Boolean requestCache() {
            return this.requestCache;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (!name.equals(entry.name)) return false;
            if (!Arrays.equals(types, entry.types)) return false;
            if (!source.equals(entry.source)) return false;
            return Objects.equals(requestCache, entry.requestCache);

        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + Arrays.hashCode(types);
            result = 31 * result + source.hashCode();
            result = 31 * result + (requestCache != null ? requestCache.hashCode() : 0);
            return result;
        }
    }

    private final List<Entry> entries;


    public IndexWarmersMetaData(Entry... entries) {
        this.entries = Arrays.asList(entries);
    }

    public List<Entry> entries() {
        return this.entries;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public IndexWarmersMetaData readFrom(StreamInput in) throws IOException {
        Entry[] entries = new Entry[in.readVInt()];
        for (int i = 0; i < entries.length; i++) {
            String name = in.readString();
            String[] types = in.readStringArray();
            BytesReference source = null;
            if (in.readBoolean()) {
                source = in.readBytesReference();
            }
            Boolean queryCache;
            queryCache = in.readOptionalBoolean();
            entries[i] = new Entry(name, types, queryCache, source);
        }
        return new IndexWarmersMetaData(entries);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(entries().size());
        for (Entry entry : entries()) {
            out.writeString(entry.name());
            out.writeStringArray(entry.types());
            if (entry.source() == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeBytesReference(entry.source());
            }
            out.writeOptionalBoolean(entry.requestCache());
        }
    }

    @Override
    public IndexWarmersMetaData fromMap(Map<String, Object> map) throws IOException {
        // if it starts with the type, remove it
        if (map.size() == 1 && map.containsKey(TYPE)) {
            map = (Map<String, Object>) map.values().iterator().next();
        }
        XContentBuilder builder = XContentFactory.smileBuilder().map(map);
        try (XContentParser parser = XContentFactory.xContent(XContentType.SMILE).createParser(builder.bytes())) {
            // move to START_OBJECT
            parser.nextToken();
            return fromXContent(parser);
        }
    }

    @Override
    public IndexWarmersMetaData fromXContent(XContentParser parser) throws IOException {
        // we get here after we are at warmers token
        String currentFieldName = null;
        XContentParser.Token token;
        List<Entry> entries = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                String name = currentFieldName;
                List<String> types = new ArrayList<>(2);
                BytesReference source = null;
                Boolean queryCache = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if ("types".equals(currentFieldName)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                types.add(parser.text());
                            }
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("source".equals(currentFieldName)) {
                            XContentBuilder builder = XContentFactory.jsonBuilder().map(parser.mapOrdered());
                            source = builder.bytes();
                        }
                    } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                        if ("source".equals(currentFieldName)) {
                            source = new BytesArray(parser.binaryValue());
                        }
                    } else if (token.isValue()) {
                        if ("requestCache".equals(currentFieldName) || "request_cache".equals(currentFieldName)) {
                            queryCache = parser.booleanValue();
                        }
                    }
                }
                entries.add(new Entry(name, types.size() == 0 ? Strings.EMPTY_ARRAY : types.toArray(new String[types.size()]), queryCache, source));
            }
        }
        return new IndexWarmersMetaData(entries.toArray(new Entry[entries.size()]));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        //No need, IndexMetaData already writes it
        //builder.startObject(TYPE, XContentBuilder.FieldCaseConversion.NONE);
        for (Entry entry : entries()) {
            toXContent(entry, builder, params);
        }
        //No need, IndexMetaData already writes it
        //builder.endObject();
        return builder;
    }

    public static void toXContent(Entry entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        boolean binary = params.paramAsBoolean("binary", false);
        builder.startObject(entry.name(), XContentBuilder.FieldCaseConversion.NONE);
        builder.field("types", entry.types());
        if (entry.requestCache() != null) {
            builder.field("requestCache", entry.requestCache());
        }
        builder.field("source");
        if (binary) {
            builder.value(entry.source());
        } else {
            Map<String, Object> mapping;
            try (XContentParser parser = XContentFactory.xContent(entry.source()).createParser(entry.source())) {
                mapping = parser.mapOrdered();
            }
            builder.map(mapping);
        }
        builder.endObject();
    }

    @Override
    public IndexMetaData.Custom mergeWith(IndexMetaData.Custom other) {
        IndexWarmersMetaData second = (IndexWarmersMetaData) other;
        List<Entry> entries = new ArrayList<>();
        entries.addAll(entries());
        for (Entry secondEntry : second.entries()) {
            boolean found = false;
            for (Entry firstEntry : entries()) {
                if (firstEntry.name().equals(secondEntry.name())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                entries.add(secondEntry);
            }
        }
        return new IndexWarmersMetaData(entries.toArray(new Entry[entries.size()]));
    }
}
