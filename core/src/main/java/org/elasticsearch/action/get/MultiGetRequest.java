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

package org.elasticsearch.action.get;

import com.google.common.collect.Iterators;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.*;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MultiGetRequest extends ActionRequest<MultiGetRequest> implements Iterable<MultiGetRequest.Item>, CompositeIndicesRequest, RealtimeRequest {

    /**
     * A single get item.
     */
    public static class Item implements Streamable, IndicesRequest {
        private String index;
        private String type;
        private String id;
        private String routing;
        private String[] fields;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;
        private FetchSourceContext fetchSourceContext;

        public Item() {

        }

        /**
         * Constructs a single get item.
         *
         * @param index The index name
         * @param type  The type (can be null)
         * @param id    The id
         */
        public Item(String index, @Nullable String type, String id) {
            this.index = index;
            this.type = type;
            this.id = id;
        }

        public String index() {
            return this.index;
        }

        @Override
        public String[] indices() {
            return new String[]{index};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return GetRequest.INDICES_OPTIONS;
        }

        public Item index(String index) {
            this.index = index;
            return this;
        }

        public String type() {
            return this.type;
        }

        public Item type(String type) {
            this.type = type;
            return this;
        }

        public String id() {
            return this.id;
        }

        /**
         * The routing associated with this document.
         */
        public Item routing(String routing) {
            this.routing = routing;
            return this;
        }

        public String routing() {
            return this.routing;
        }

        public Item parent(String parent) {
            if (routing == null) {
                this.routing = parent;
            }
            return this;
        }

        public Item fields(String... fields) {
            this.fields = fields;
            return this;
        }

        public String[] fields() {
            return this.fields;
        }

        public long version() {
            return version;
        }

        public Item version(long version) {
            this.version = version;
            return this;
        }

        public VersionType versionType() {
            return versionType;
        }

        public Item versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }

        public FetchSourceContext fetchSourceContext() {
            return this.fetchSourceContext;
        }

        /**
         * Allows setting the {@link FetchSourceContext} for this request, controlling if and how _source should be returned.
         */
        public Item fetchSourceContext(FetchSourceContext fetchSourceContext) {
            this.fetchSourceContext = fetchSourceContext;
            return this;
        }

        public static Item readItem(StreamInput in) throws IOException {
            Item item = new Item();
            item.readFrom(in);
            return item;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            index = in.readString();
            type = in.readOptionalString();
            id = in.readString();
            routing = in.readOptionalString();
            int size = in.readVInt();
            if (size > 0) {
                fields = new String[size];
                for (int i = 0; i < size; i++) {
                    fields[i] = in.readString();
                }
            }
            version = in.readLong();
            versionType = VersionType.fromValue(in.readByte());

            fetchSourceContext = FetchSourceContext.optionalReadFromStream(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeOptionalString(type);
            out.writeString(id);
            out.writeOptionalString(routing);
            if (fields == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(fields.length);
                for (String field : fields) {
                    out.writeString(field);
                }
            }

            out.writeLong(version);
            out.writeByte(versionType.getValue());

            FetchSourceContext.optionalWriteToStream(fetchSourceContext, out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Item)) return false;

            Item item = (Item) o;

            if (version != item.version) return false;
            if (fetchSourceContext != null ? !fetchSourceContext.equals(item.fetchSourceContext) : item.fetchSourceContext != null)
                return false;
            if (!Arrays.equals(fields, item.fields)) return false;
            if (!id.equals(item.id)) return false;
            if (!index.equals(item.index)) return false;
            if (routing != null ? !routing.equals(item.routing) : item.routing != null) return false;
            if (type != null ? !type.equals(item.type) : item.type != null) return false;
            if (versionType != item.versionType) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = index.hashCode();
            result = 31 * result + (type != null ? type.hashCode() : 0);
            result = 31 * result + id.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (fields != null ? Arrays.hashCode(fields) : 0);
            result = 31 * result + (int) (version ^ (version >>> 32));
            result = 31 * result + versionType.hashCode();
            result = 31 * result + (fetchSourceContext != null ? fetchSourceContext.hashCode() : 0);
            return result;
        }
    }

    String preference;
    Boolean realtime;
    boolean refresh;
    public boolean ignoreErrorsOnGeneratedFields = false;

    List<Item> items = new ArrayList<>();

    public MultiGetRequest() {

    }

    /**
     * Creates a multi get request caused by some other request, which is provided as an
     * argument so that its headers and context can be copied to the new request
     */
    public MultiGetRequest(ActionRequest request) {
        super(request);
    }

    public List<Item> getItems() {
        return this.items;
    }

    public MultiGetRequest add(Item item) {
        items.add(item);
        return this;
    }

    public MultiGetRequest add(String index, @Nullable String type, String id) {
        items.add(new Item(index, type, id));
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (items.isEmpty()) {
            validationException = ValidateActions.addValidationError("no documents to get", validationException);
        } else {
            for (int i = 0; i < items.size(); i++) {
                Item item = items.get(i);
                if (item.index() == null) {
                    validationException = ValidateActions.addValidationError("index is missing for doc " + i, validationException);
                }
                if (item.id() == null) {
                    validationException = ValidateActions.addValidationError("id is missing for doc " + i, validationException);
                }
            }
        }
        return validationException;
    }

    @Override
    public List<? extends IndicesRequest> subRequests() {
        return items;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public MultiGetRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    public boolean realtime() {
        return this.realtime == null ? true : this.realtime;
    }

    @Override
    public MultiGetRequest realtime(Boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public MultiGetRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }


    public MultiGetRequest ignoreErrorsOnGeneratedFields(boolean ignoreErrorsOnGeneratedFields) {
        this.ignoreErrorsOnGeneratedFields = ignoreErrorsOnGeneratedFields;
        return this;
    }

    public MultiGetRequest add(@Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSource, byte[] data, int from, int length) throws Exception {
        return add(defaultIndex, defaultType, defaultFields, defaultFetchSource, new BytesArray(data, from, length), true);
    }

    public MultiGetRequest add(@Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSource, BytesReference data) throws Exception {
        return add(defaultIndex, defaultType, defaultFields, defaultFetchSource, data, true);
    }

    public MultiGetRequest add(@Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSource, BytesReference data, boolean allowExplicitIndex) throws Exception {
        return add(defaultIndex, defaultType, defaultFields, defaultFetchSource, null, data, allowExplicitIndex);
    }

    public MultiGetRequest add(@Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSource, @Nullable String defaultRouting, BytesReference data, boolean allowExplicitIndex) throws Exception {
        try (XContentParser parser = XContentFactory.xContent(data).createParser(data)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("docs".equals(currentFieldName)) {
                        parseDocuments(parser, this.items, defaultIndex, defaultType, defaultFields, defaultFetchSource, defaultRouting, allowExplicitIndex);
                    } else if ("ids".equals(currentFieldName)) {
                        parseIds(parser, this.items, defaultIndex, defaultType, defaultFields, defaultFetchSource, defaultRouting);
                    }
                }
            }
        }
        return this;
    }

    public static void parseDocuments(XContentParser parser, List<Item> items, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSource, @Nullable String defaultRouting, boolean allowExplicitIndex) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("docs array element should include an object");
            }
            String index = defaultIndex;
            String type = defaultType;
            String id = null;
            String routing = defaultRouting;
            String parent = null;
            List<String> fields = null;
            long version = Versions.MATCH_ANY;
            VersionType versionType = VersionType.INTERNAL;

            FetchSourceContext fetchSourceContext = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("_index".equals(currentFieldName)) {
                        if (!allowExplicitIndex) {
                            throw new IllegalArgumentException("explicit index in multi get is not allowed");
                        }
                        index = parser.text();
                    } else if ("_type".equals(currentFieldName)) {
                        type = parser.text();
                    } else if ("_id".equals(currentFieldName)) {
                        id = parser.text();
                    } else if ("_routing".equals(currentFieldName) || "routing".equals(currentFieldName)) {
                        routing = parser.text();
                    } else if ("_parent".equals(currentFieldName) || "parent".equals(currentFieldName)) {
                        parent = parser.text();
                    } else if ("fields".equals(currentFieldName)) {
                        fields = new ArrayList<>();
                        fields.add(parser.text());
                    } else if ("_version".equals(currentFieldName) || "version".equals(currentFieldName)) {
                        version = parser.longValue();
                    } else if ("_version_type".equals(currentFieldName) || "_versionType".equals(currentFieldName) || "version_type".equals(currentFieldName) || "versionType".equals(currentFieldName)) {
                        versionType = VersionType.fromString(parser.text());
                    } else if ("_source".equals(currentFieldName)) {
                        if (parser.isBooleanValue()) {
                            fetchSourceContext = new FetchSourceContext(parser.booleanValue());
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            fetchSourceContext = new FetchSourceContext(new String[]{parser.text()});
                        } else {
                            throw new ElasticsearchParseException("illegal type for _source: [{}]", token);
                        }
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("fields".equals(currentFieldName)) {
                        fields = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            fields.add(parser.text());
                        }
                    } else if ("_source".equals(currentFieldName)) {
                        ArrayList<String> includes = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            includes.add(parser.text());
                        }
                        fetchSourceContext = new FetchSourceContext(includes.toArray(Strings.EMPTY_ARRAY));
                    }

                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_source".equals(currentFieldName)) {
                        List<String> currentList = null, includes = null, excludes = null;

                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                                if ("includes".equals(currentFieldName) || "include".equals(currentFieldName)) {
                                    currentList = includes != null ? includes : (includes = new ArrayList<>(2));
                                } else if ("excludes".equals(currentFieldName) || "exclude".equals(currentFieldName)) {
                                    currentList = excludes != null ? excludes : (excludes = new ArrayList<>(2));
                                } else {
                                    throw new ElasticsearchParseException("source definition may not contain [{}]", parser.text());
                                }
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    currentList.add(parser.text());
                                }
                            } else if (token.isValue()) {
                                currentList.add(parser.text());
                            } else {
                                throw new ElasticsearchParseException("unexpected token while parsing source settings");
                            }
                        }

                        fetchSourceContext = new FetchSourceContext(
                                includes == null ? Strings.EMPTY_ARRAY : includes.toArray(new String[includes.size()]),
                                excludes == null ? Strings.EMPTY_ARRAY : excludes.toArray(new String[excludes.size()]));
                    }
                }
            }
            String[] aFields;
            if (fields != null) {
                aFields = fields.toArray(new String[fields.size()]);
            } else {
                aFields = defaultFields;
            }
            items.add(new Item(index, type, id).routing(routing).fields(aFields).parent(parent).version(version).versionType(versionType)
                    .fetchSourceContext(fetchSourceContext == null ? defaultFetchSource : fetchSourceContext));
        }
    }

    public static void parseDocuments(XContentParser parser, List<Item> items) throws IOException {
        parseDocuments(parser, items, null, null, null, null, null, true);
    }

    public static void parseIds(XContentParser parser, List<Item> items, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSource, @Nullable String defaultRouting) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (!token.isValue()) {
                throw new IllegalArgumentException("ids array element should only contain ids");
            }
            items.add(new Item(defaultIndex, defaultType, parser.text()).fields(defaultFields).fetchSourceContext(defaultFetchSource).routing(defaultRouting));
        }
    }

    public static void parseIds(XContentParser parser, List<Item> items) throws IOException {
        parseIds(parser, items, null, null, null, null, null);
    }

    @Override
    public Iterator<Item> iterator() {
        return Iterators.unmodifiableIterator(items.iterator());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        preference = in.readOptionalString();
        refresh = in.readBoolean();
        byte realtime = in.readByte();
        if (realtime == 0) {
            this.realtime = false;
        } else if (realtime == 1) {
            this.realtime = true;
        }
        ignoreErrorsOnGeneratedFields = in.readBoolean();

        int size = in.readVInt();
        items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(Item.readItem(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeBoolean(refresh);
        if (realtime == null) {
            out.writeByte((byte) -1);
        } else if (realtime == false) {
            out.writeByte((byte) 0);
        } else {
            out.writeByte((byte) 1);
        }
        out.writeBoolean(ignoreErrorsOnGeneratedFields);

        out.writeVInt(items.size());
        for (Item item : items) {
            item.writeTo(out);
        }
    }
}
