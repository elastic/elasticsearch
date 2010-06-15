/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.internal;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.trove.TIntObjectHashMap;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.lucene.Lucene.*;
import static org.elasticsearch.search.SearchShardTarget.*;
import static org.elasticsearch.search.highlight.HighlightField.*;
import static org.elasticsearch.search.internal.InternalSearchHitField.*;

/**
 * @author kimchy (shay.banon)
 */
public class InternalSearchHit implements SearchHit {

    private transient int docId;

    private String id;

    private String type;

    private byte[] source;

    private Map<String, SearchHitField> fields = ImmutableMap.of();

    private Map<String, HighlightField> highlightFields = ImmutableMap.of();

    private Explanation explanation;

    @Nullable private SearchShardTarget shard;

    private Map<String, Object> sourceAsMap;

    private InternalSearchHit() {

    }

    public InternalSearchHit(int docId, String id, String type, byte[] source, Map<String, SearchHitField> fields) {
        this.docId = docId;
        this.id = id;
        this.type = type;
        this.source = source;
        this.fields = fields;
    }

    public int docId() {
        return this.docId;
    }

    @Override public String index() {
        return shard.index();
    }

    @Override public String getIndex() {
        return index();
    }

    @Override public String id() {
        return id;
    }

    @Override public String getId() {
        return id();
    }

    @Override public String type() {
        return type;
    }

    @Override public String getType() {
        return type();
    }

    @Override public byte[] source() {
        return source;
    }

    @Override public Map<String, Object> getSource() {
        return sourceAsMap();
    }

    @Override public String sourceAsString() {
        if (source == null) {
            return null;
        }
        return Unicode.fromBytes(source);
    }

    @SuppressWarnings({"unchecked"})
    @Override public Map<String, Object> sourceAsMap() throws ElasticSearchParseException {
        if (source == null) {
            return null;
        }
        if (sourceAsMap != null) {
            return sourceAsMap;
        }
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            sourceAsMap = parser.map();
            parser.close();
            return sourceAsMap;
        } catch (Exception e) {
            throw new ElasticSearchParseException("Failed to parse source to map", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    @Override public Iterator<SearchHitField> iterator() {
        return fields.values().iterator();
    }

    @Override public Map<String, SearchHitField> fields() {
        return fields;
    }

    @Override public Map<String, SearchHitField> getFields() {
        return fields();
    }

    public void fields(Map<String, SearchHitField> fields) {
        this.fields = fields;
    }

    @Override public Map<String, HighlightField> highlightFields() {
        return this.highlightFields;
    }

    @Override public Map<String, HighlightField> getHighlightFields() {
        return highlightFields();
    }

    public void highlightFields(Map<String, HighlightField> highlightFields) {
        this.highlightFields = highlightFields;
    }

    @Override public Explanation explanation() {
        return explanation;
    }

    @Override public Explanation getExplanation() {
        return explanation();
    }

    public void explanation(Explanation explanation) {
        this.explanation = explanation;
    }

    @Override public SearchShardTarget shard() {
        return shard;
    }

    @Override public SearchShardTarget getShard() {
        return shard();
    }

    public void shard(SearchShardTarget target) {
        this.shard = target;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("_index", shard.index());
//        builder.field("_shard", shard.shardId());
//        builder.field("_node", shard.nodeId());
        builder.field("_type", type());
        builder.field("_id", id());
        if (source() != null) {
            if (XContentFactory.xContentType(source()) == builder.contentType()) {
                builder.rawField("_source", source());
            } else {
                builder.field("_source");
                builder.value(source());
            }
        }
        if (fields != null && !fields.isEmpty()) {
            builder.startObject("fields");
            for (SearchHitField field : fields.values()) {
                if (field.values().isEmpty()) {
                    continue;
                }
                if (field.values().size() == 1) {
                    builder.field(field.name(), field.values().get(0));
                } else {
                    builder.field(field.name());
                    builder.startArray();
                    for (Object value : field.values()) {
                        builder.value(value);
                    }
                    builder.endArray();
                }
            }
            builder.endObject();
        }
        if (highlightFields != null && !highlightFields.isEmpty()) {
            builder.startObject("highlight");
            for (HighlightField field : highlightFields.values()) {
                builder.field(field.name());
                if (field.fragments() == null) {
                    builder.nullValue();
                } else {
                    builder.startArray();
                    for (String fragment : field.fragments()) {
                        builder.value(fragment);
                    }
                    builder.endArray();
                }
            }
            builder.endObject();
        }
        if (explanation() != null) {
            builder.field("_explanation");
            buildExplanation(builder, explanation());
        }
        builder.endObject();
    }

    private void buildExplanation(XContentBuilder builder, Explanation explanation) throws IOException {
        builder.startObject();
        builder.field("value", explanation.getValue());
        builder.field("description", explanation.getDescription());
        Explanation[] innerExps = explanation.getDetails();
        if (innerExps != null) {
            builder.startArray("details");
            for (Explanation exp : innerExps) {
                buildExplanation(builder, exp);
            }
            builder.endArray();
        }
        builder.endObject();
    }

    public static InternalSearchHit readSearchHit(StreamInput in) throws IOException {
        InternalSearchHit hit = new InternalSearchHit();
        hit.readFrom(in);
        return hit;
    }

    public static InternalSearchHit readSearchHit(StreamInput in, @Nullable TIntObjectHashMap<SearchShardTarget> shardLookupMap) throws IOException {
        InternalSearchHit hit = new InternalSearchHit();
        hit.readFrom(in, shardLookupMap);
        return hit;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        readFrom(in, null);
    }

    public void readFrom(StreamInput in, @Nullable TIntObjectHashMap<SearchShardTarget> shardLookupMap) throws IOException {
        id = in.readUTF();
        type = in.readUTF();
        int size = in.readVInt();
        if (size > 0) {
            source = new byte[size];
            in.readFully(source);
        }
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        size = in.readVInt();
        if (size == 0) {
            fields = ImmutableMap.of();
        } else if (size == 1) {
            SearchHitField hitField = readSearchHitField(in);
            fields = ImmutableMap.of(hitField.name(), hitField);
        } else if (size == 2) {
            SearchHitField hitField1 = readSearchHitField(in);
            SearchHitField hitField2 = readSearchHitField(in);
            fields = ImmutableMap.of(hitField1.name(), hitField1, hitField2.name(), hitField2);
        } else if (size == 3) {
            SearchHitField hitField1 = readSearchHitField(in);
            SearchHitField hitField2 = readSearchHitField(in);
            SearchHitField hitField3 = readSearchHitField(in);
            fields = ImmutableMap.of(hitField1.name(), hitField1, hitField2.name(), hitField2, hitField3.name(), hitField3);
        } else if (size == 4) {
            SearchHitField hitField1 = readSearchHitField(in);
            SearchHitField hitField2 = readSearchHitField(in);
            SearchHitField hitField3 = readSearchHitField(in);
            SearchHitField hitField4 = readSearchHitField(in);
            fields = ImmutableMap.of(hitField1.name(), hitField1, hitField2.name(), hitField2, hitField3.name(), hitField3, hitField4.name(), hitField4);
        } else if (size == 5) {
            SearchHitField hitField1 = readSearchHitField(in);
            SearchHitField hitField2 = readSearchHitField(in);
            SearchHitField hitField3 = readSearchHitField(in);
            SearchHitField hitField4 = readSearchHitField(in);
            SearchHitField hitField5 = readSearchHitField(in);
            fields = ImmutableMap.of(hitField1.name(), hitField1, hitField2.name(), hitField2, hitField3.name(), hitField3, hitField4.name(), hitField4, hitField5.name(), hitField5);
        } else {
            ImmutableMap.Builder<String, SearchHitField> builder = ImmutableMap.builder();
            for (int i = 0; i < size; i++) {
                SearchHitField hitField = readSearchHitField(in);
                builder.put(hitField.name(), hitField);
            }
            fields = builder.build();
        }

        size = in.readVInt();
        if (size == 0) {
            highlightFields = ImmutableMap.of();
        } else if (size == 1) {
            HighlightField field = readHighlightField(in);
            highlightFields = ImmutableMap.of(field.name(), field);
        } else if (size == 2) {
            HighlightField field1 = readHighlightField(in);
            HighlightField field2 = readHighlightField(in);
            highlightFields = ImmutableMap.of(field1.name(), field1, field2.name(), field2);
        } else if (size == 3) {
            HighlightField field1 = readHighlightField(in);
            HighlightField field2 = readHighlightField(in);
            HighlightField field3 = readHighlightField(in);
            highlightFields = ImmutableMap.of(field1.name(), field1, field2.name(), field2, field3.name(), field3);
        } else if (size == 4) {
            HighlightField field1 = readHighlightField(in);
            HighlightField field2 = readHighlightField(in);
            HighlightField field3 = readHighlightField(in);
            HighlightField field4 = readHighlightField(in);
            highlightFields = ImmutableMap.of(field1.name(), field1, field2.name(), field2, field3.name(), field3, field4.name(), field4);
        } else {
            ImmutableMap.Builder<String, HighlightField> builder = ImmutableMap.builder();
            for (int i = 0; i < size; i++) {
                HighlightField field = readHighlightField(in);
                builder.put(field.name(), field);
            }
            highlightFields = builder.build();
        }

        if (shardLookupMap != null) {
            int lookupId = in.readVInt();
            if (lookupId > 0) {
                shard = shardLookupMap.get(lookupId);
            }
        } else {
            if (in.readBoolean()) {
                shard = readSearchShardTarget(in);
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        writeTo(out, null);
    }

    public void writeTo(StreamOutput out, @Nullable Map<SearchShardTarget, Integer> shardLookupMap) throws IOException {
        out.writeUTF(id);
        out.writeUTF(type);
        if (source == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(source.length);
            out.writeBytes(source);
        }
        if (explanation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeExplanation(out, explanation);
        }
        if (fields == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(fields.size());
            for (SearchHitField hitField : fields().values()) {
                hitField.writeTo(out);
            }
        }
        if (highlightFields == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(highlightFields.size());
            for (HighlightField highlightField : highlightFields.values()) {
                highlightField.writeTo(out);
            }
        }
        if (shardLookupMap == null) {
            if (shard == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                shard.writeTo(out);
            }
        } else {
            if (shard == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(shardLookupMap.get(shard));
            }
        }
    }
}