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

package org.elasticsearch.search.internal;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.lucene.Lucene.readExplanation;
import static org.elasticsearch.common.lucene.Lucene.writeExplanation;
import static org.elasticsearch.search.SearchShardTarget.readSearchShardTarget;
import static org.elasticsearch.search.highlight.HighlightField.readHighlightField;
import static org.elasticsearch.search.internal.InternalSearchHitField.readSearchHitField;

/**
 *
 */
public class InternalSearchHit implements SearchHit {

    private static final Object[] EMPTY_SORT_VALUES = new Object[0];
    private static final Text MAX_TERM_AS_TEXT = new StringAndBytesText(BytesRefFieldComparatorSource.MAX_TERM.utf8ToString());

    private transient int docId;

    private float score = Float.NEGATIVE_INFINITY;

    private Text id;
    private Text type;

    private InternalNestedIdentity nestedIdentity;

    private long version = -1;

    private BytesReference source;

    private Map<String, SearchHitField> fields = ImmutableMap.of();

    private Map<String, HighlightField> highlightFields = null;

    private Object[] sortValues = EMPTY_SORT_VALUES;

    private String[] matchedQueries = Strings.EMPTY_ARRAY;

    private Explanation explanation;

    @Nullable
    private SearchShardTarget shard;

    private Map<String, Object> sourceAsMap;
    private byte[] sourceAsBytes;

    private Map<String, InternalSearchHits> innerHits;

    private InternalSearchHit() {

    }

    public InternalSearchHit(int docId, String id, Text type, Map<String, SearchHitField> fields) {
        this.docId = docId;
        this.id = new StringAndBytesText(id);
        this.type = type;
        this.fields = fields;
    }

    public InternalSearchHit(int nestedTopDocId, String id, Text type, InternalNestedIdentity nestedIdentity, Map<String, SearchHitField> fields) {
        this.docId = nestedTopDocId;
        this.id = new StringAndBytesText(id);
        this.type = type;
        this.nestedIdentity = nestedIdentity;
        this.fields = fields;
    }

    public int docId() {
        return this.docId;
    }

    public void shardTarget(SearchShardTarget shardTarget) {
        this.shard = shardTarget;
        if (innerHits != null) {
            for (InternalSearchHits searchHits : innerHits.values()) {
                searchHits.shardTarget(shardTarget);
            }
        }
    }

    public void score(float score) {
        this.score = score;
    }

    @Override
    public float score() {
        return this.score;
    }

    @Override
    public float getScore() {
        return score();
    }

    public void version(long version) {
        this.version = version;
    }

    @Override
    public long version() {
        return this.version;
    }

    @Override
    public long getVersion() {
        return this.version;
    }

    @Override
    public String index() {
        return shard.index();
    }

    @Override
    public String getIndex() {
        return index();
    }

    @Override
    public String id() {
        return id.string();
    }

    @Override
    public String getId() {
        return id();
    }

    @Override
    public String type() {
        return type.string();
    }

    @Override
    public String getType() {
        return type();
    }

    @Override
    public NestedIdentity getNestedIdentity() {
        return nestedIdentity;
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    @Override
    public BytesReference sourceRef() {
        try {
            this.source = CompressorFactory.uncompressIfNeeded(this.source);
            return this.source;
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to decompress source", e);
        }
    }

    /**
     * Sets representation, might be compressed....
     */
    public InternalSearchHit sourceRef(BytesReference source) {
        this.source = source;
        this.sourceAsBytes = null;
        this.sourceAsMap = null;
        return this;
    }

    @Override
    public BytesReference getSourceRef() {
        return sourceRef();
    }

    /**
     * Internal source representation, might be compressed....
     */
    public BytesReference internalSourceRef() {
        return source;
    }


    @Override
    public byte[] source() {
        if (source == null) {
            return null;
        }
        if (sourceAsBytes != null) {
            return sourceAsBytes;
        }
        this.sourceAsBytes = sourceRef().toBytes();
        return this.sourceAsBytes;
    }

    @Override
    public boolean isSourceEmpty() {
        return source == null;
    }

    @Override
    public Map<String, Object> getSource() {
        return sourceAsMap();
    }

    @Override
    public String sourceAsString() {
        if (source == null) {
            return null;
        }
        try {
            return XContentHelper.convertToJson(sourceRef(), false);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to convert source to a json string");
        }
    }

    @Override
    public String getSourceAsString() {
        return sourceAsString();
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Map<String, Object> sourceAsMap() throws ElasticsearchParseException {
        if (source == null) {
            return null;
        }
        if (sourceAsMap != null) {
            return sourceAsMap;
        }

        sourceAsMap = SourceLookup.sourceAsMap(source);
        return sourceAsMap;
    }

    @Override
    public Iterator<SearchHitField> iterator() {
        return fields.values().iterator();
    }

    @Override
    public SearchHitField field(String fieldName) {
        return fields().get(fieldName);
    }

    @Override
    public Map<String, SearchHitField> fields() {
        if (fields == null) {
            return ImmutableMap.of();
        }
        return fields;
    }

    // returns the fields without handling null cases
    public Map<String, SearchHitField> fieldsOrNull() {
        return this.fields;
    }

    @Override
    public Map<String, SearchHitField> getFields() {
        return fields();
    }

    public void fields(Map<String, SearchHitField> fields) {
        this.fields = fields;
    }

    public Map<String, HighlightField> internalHighlightFields() {
        return highlightFields;
    }

    @Override
    public Map<String, HighlightField> highlightFields() {
        if (highlightFields == null) {
            return ImmutableMap.of();
        }
        return this.highlightFields;
    }

    @Override
    public Map<String, HighlightField> getHighlightFields() {
        return highlightFields();
    }

    public void highlightFields(Map<String, HighlightField> highlightFields) {
        this.highlightFields = highlightFields;
    }

    public void sortValues(Object[] sortValues) {
        // LUCENE 4 UPGRADE: There must be a better way
        // we want to convert to a Text object here, and not BytesRef

        // Don't write into sortValues! Otherwise the fields in FieldDoc is modified, which may be used in other places. (SearchContext#lastEmitedDoc)
        Object[] sortValuesCopy = new Object[sortValues.length];
        System.arraycopy(sortValues, 0, sortValuesCopy, 0, sortValues.length);
        if (sortValues != null) {
            for (int i = 0; i < sortValues.length; i++) {
                if (sortValues[i] instanceof BytesRef) {
                    sortValuesCopy[i] = new StringAndBytesText(new BytesArray((BytesRef) sortValues[i]));
                }
            }
        }
        this.sortValues = sortValuesCopy;
    }

    @Override
    public Object[] sortValues() {
        return sortValues;
    }

    @Override
    public Object[] getSortValues() {
        return sortValues();
    }

    @Override
    public Explanation explanation() {
        return explanation;
    }

    @Override
    public Explanation getExplanation() {
        return explanation();
    }

    public void explanation(Explanation explanation) {
        this.explanation = explanation;
    }

    @Override
    public SearchShardTarget shard() {
        return shard;
    }

    @Override
    public SearchShardTarget getShard() {
        return shard();
    }

    public void shard(SearchShardTarget target) {
        this.shard = target;
    }

    public void matchedQueries(String[] matchedQueries) {
        this.matchedQueries = matchedQueries;
    }

    @Override
    public String[] matchedQueries() {
        return this.matchedQueries;
    }

    @Override
    public String[] getMatchedQueries() {
        return this.matchedQueries;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, SearchHits> getInnerHits() {
        return (Map) innerHits;
    }

    public void setInnerHits(Map<String, InternalSearchHits> innerHits) {
        this.innerHits = innerHits;
    }

    public static class Fields {
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString _SCORE = new XContentBuilderString("_score");
        static final XContentBuilderString FIELDS = new XContentBuilderString("fields");
        static final XContentBuilderString HIGHLIGHT = new XContentBuilderString("highlight");
        static final XContentBuilderString SORT = new XContentBuilderString("sort");
        static final XContentBuilderString MATCHED_QUERIES = new XContentBuilderString("matched_queries");
        static final XContentBuilderString _EXPLANATION = new XContentBuilderString("_explanation");
        static final XContentBuilderString VALUE = new XContentBuilderString("value");
        static final XContentBuilderString DESCRIPTION = new XContentBuilderString("description");
        static final XContentBuilderString DETAILS = new XContentBuilderString("details");
        static final XContentBuilderString INNER_HITS = new XContentBuilderString("inner_hits");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // For inner_hit hits shard is null and that is ok, because the parent search hit has all this information.
        // Even if this was included in the inner_hit hits this would be the same, so better leave it out.
        if (explanation() != null && shard != null) {
            builder.field("_shard", shard.shardId());
            builder.field("_node", shard.nodeIdText());
        }
        if (shard != null) {
            builder.field(Fields._INDEX, shard.indexText());
        }
        builder.field(Fields._TYPE, type);
        builder.field(Fields._ID, id);
        if (nestedIdentity != null) {
            nestedIdentity.toXContent(builder, params);
        }
        if (version != -1) {
            builder.field(Fields._VERSION, version);
        }
        if (Float.isNaN(score)) {
            builder.nullField(Fields._SCORE);
        } else {
            builder.field(Fields._SCORE, score);
        }
        if (source != null) {
            XContentHelper.writeRawField("_source", source, builder, params);
        }
        if (fields != null && !fields.isEmpty()) {
            builder.startObject(Fields.FIELDS);
            for (SearchHitField field : fields.values()) {
                if (field.values().isEmpty()) {
                    continue;
                }
                String fieldName = field.getName();
                if (field.isMetadataField()) {
                    builder.field(fieldName, field.value());
                } else {
                    builder.startArray(fieldName);
                    for (Object value : field.getValues()) {
                        builder.value(value);
                    }
                    builder.endArray();
                }
            }
            builder.endObject();
        }
        if (highlightFields != null && !highlightFields.isEmpty()) {
            builder.startObject(Fields.HIGHLIGHT);
            for (HighlightField field : highlightFields.values()) {
                builder.field(field.name());
                if (field.fragments() == null) {
                    builder.nullValue();
                } else {
                    builder.startArray();
                    for (Text fragment : field.fragments()) {
                        builder.value(fragment);
                    }
                    builder.endArray();
                }
            }
            builder.endObject();
        }
        if (sortValues != null && sortValues.length > 0) {
            builder.startArray(Fields.SORT);
            for (Object sortValue : sortValues) {
                if (sortValue != null && sortValue.equals(MAX_TERM_AS_TEXT)) {
                    // We don't display MAX_TERM in JSON responses in case some clients have UTF-8 parsers that wouldn't accept a
                    // non-character in the response, even though this is valid UTF-8
                    builder.nullValue();
                } else {
                    builder.value(sortValue);
                }
            }
            builder.endArray();
        }
        if (matchedQueries.length > 0) {
            builder.startArray(Fields.MATCHED_QUERIES);
            for (String matchedFilter : matchedQueries) {
                builder.value(matchedFilter);
            }
            builder.endArray();
        }
        if (explanation() != null) {
            builder.field(Fields._EXPLANATION);
            buildExplanation(builder, explanation());
        }
        if (innerHits != null) {
            builder.startObject(Fields.INNER_HITS);
            for (Map.Entry<String, InternalSearchHits> entry : innerHits.entrySet()) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private void buildExplanation(XContentBuilder builder, Explanation explanation) throws IOException {
        builder.startObject();
        builder.field(Fields.VALUE, explanation.getValue());
        builder.field(Fields.DESCRIPTION, explanation.getDescription());
        Explanation[] innerExps = explanation.getDetails();
        if (innerExps != null) {
            builder.startArray(Fields.DETAILS);
            for (Explanation exp : innerExps) {
                buildExplanation(builder, exp);
            }
            builder.endArray();
        }
        builder.endObject();
    }

    public static InternalSearchHit readSearchHit(StreamInput in, InternalSearchHits.StreamContext context) throws IOException {
        InternalSearchHit hit = new InternalSearchHit();
        hit.readFrom(in, context);
        return hit;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        readFrom(in, InternalSearchHits.streamContext().streamShardTarget(InternalSearchHits.StreamContext.ShardTargetType.STREAM));
    }

    public void readFrom(StreamInput in, InternalSearchHits.StreamContext context) throws IOException {
        score = in.readFloat();
        id = in.readText();
        type = in.readText();
        nestedIdentity = in.readOptionalStreamable(new InternalNestedIdentity());
        version = in.readLong();
        source = in.readBytesReference();
        if (source.length() == 0) {
            source = null;
        }
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        int size = in.readVInt();
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

        size = in.readVInt();
        if (size > 0) {
            sortValues = new Object[size];
            for (int i = 0; i < sortValues.length; i++) {
                byte type = in.readByte();
                if (type == 0) {
                    sortValues[i] = null;
                } else if (type == 1) {
                    sortValues[i] = in.readString();
                } else if (type == 2) {
                    sortValues[i] = in.readInt();
                } else if (type == 3) {
                    sortValues[i] = in.readLong();
                } else if (type == 4) {
                    sortValues[i] = in.readFloat();
                } else if (type == 5) {
                    sortValues[i] = in.readDouble();
                } else if (type == 6) {
                    sortValues[i] = in.readByte();
                } else if (type == 7) {
                    sortValues[i] = in.readShort();
                } else if (type == 8) {
                    sortValues[i] = in.readBoolean();
                } else if (type == 9) {
                    sortValues[i] = in.readText();
                } else {
                    throw new IOException("Can't match type [" + type + "]");
                }
            }
        }

        size = in.readVInt();
        if (size > 0) {
            matchedQueries = new String[size];
            for (int i = 0; i < size; i++) {
                matchedQueries[i] = in.readString();
            }
        }

        if (context.streamShardTarget() == InternalSearchHits.StreamContext.ShardTargetType.STREAM) {
            if (in.readBoolean()) {
                shard = readSearchShardTarget(in);
            }
        } else if (context.streamShardTarget() == InternalSearchHits.StreamContext.ShardTargetType.LOOKUP) {
            int lookupId = in.readVInt();
            if (lookupId > 0) {
                shard = context.handleShardLookup().get(lookupId);
            }
        }

        size = in.readVInt();
        if (size > 0) {
            innerHits = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                InternalSearchHits value = InternalSearchHits.readSearchHits(in, InternalSearchHits.streamContext().streamShardTarget(InternalSearchHits.StreamContext.ShardTargetType.NO_STREAM));
                innerHits.put(key, value);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeTo(out, InternalSearchHits.streamContext().streamShardTarget(InternalSearchHits.StreamContext.ShardTargetType.STREAM));
    }

    public void writeTo(StreamOutput out, InternalSearchHits.StreamContext context) throws IOException {
        out.writeFloat(score);
        out.writeText(id);
        out.writeText(type);
        out.writeOptionalStreamable(nestedIdentity);
        out.writeLong(version);
        out.writeBytesReference(source);
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

        if (sortValues.length == 0) {
            out.writeVInt(0);
        } else {
            out.writeVInt(sortValues.length);
            for (Object sortValue : sortValues) {
                if (sortValue == null) {
                    out.writeByte((byte) 0);
                } else {
                    Class type = sortValue.getClass();
                    if (type == String.class) {
                        out.writeByte((byte) 1);
                        out.writeString((String) sortValue);
                    } else if (type == Integer.class) {
                        out.writeByte((byte) 2);
                        out.writeInt((Integer) sortValue);
                    } else if (type == Long.class) {
                        out.writeByte((byte) 3);
                        out.writeLong((Long) sortValue);
                    } else if (type == Float.class) {
                        out.writeByte((byte) 4);
                        out.writeFloat((Float) sortValue);
                    } else if (type == Double.class) {
                        out.writeByte((byte) 5);
                        out.writeDouble((Double) sortValue);
                    } else if (type == Byte.class) {
                        out.writeByte((byte) 6);
                        out.writeByte((Byte) sortValue);
                    } else if (type == Short.class) {
                        out.writeByte((byte) 7);
                        out.writeShort((Short) sortValue);
                    } else if (type == Boolean.class) {
                        out.writeByte((byte) 8);
                        out.writeBoolean((Boolean) sortValue);
                    } else if (sortValue instanceof Text) {
                        out.writeByte((byte) 9);
                        out.writeText((Text) sortValue);
                    } else {
                        throw new IOException("Can't handle sort field value of type [" + type + "]");
                    }
                }
            }
        }

        if (matchedQueries.length == 0) {
            out.writeVInt(0);
        } else {
            out.writeVInt(matchedQueries.length);
            for (String matchedFilter : matchedQueries) {
                out.writeString(matchedFilter);
            }
        }

        if (context.streamShardTarget() == InternalSearchHits.StreamContext.ShardTargetType.STREAM) {
            if (shard == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                shard.writeTo(out);
            }
        } else if (context.streamShardTarget() == InternalSearchHits.StreamContext.ShardTargetType.LOOKUP) {
            if (shard == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(context.shardHandleLookup().get(shard));
            }
        }

        if (innerHits == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(innerHits.size());
            for (Map.Entry<String, InternalSearchHits> entry : innerHits.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out, InternalSearchHits.streamContext().streamShardTarget(InternalSearchHits.StreamContext.ShardTargetType.NO_STREAM));
            }
        }
    }

    public final static class InternalNestedIdentity implements NestedIdentity, Streamable, ToXContent {

        private Text field;
        private int offset;
        private InternalNestedIdentity child;

        public InternalNestedIdentity(String field, int offset, InternalNestedIdentity child) {
            this.field = new StringAndBytesText(field);
            this.offset = offset;
            this.child = child;
        }

        InternalNestedIdentity() {
        }

        @Override
        public Text getField() {
            return field;
        }

        @Override
        public int getOffset() {
            return offset;
        }

        @Override
        public NestedIdentity getChild() {
            return child;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            field = in.readOptionalText();
            offset = in.readInt();
            child = in.readOptionalStreamable(new InternalNestedIdentity());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalText(field);
            out.writeInt(offset);
            out.writeOptionalStreamable(child);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields._NESTED);
            if (field != null) {
                builder.field(Fields._NESTED_FIELD, field);
            }
            if (offset != -1) {
                builder.field(Fields._NESTED_OFFSET, offset);
            }
            if (child != null) {
                builder = child.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        public static class Fields {

            static final XContentBuilderString _NESTED = new XContentBuilderString("_nested");
            static final XContentBuilderString _NESTED_FIELD = new XContentBuilderString("field");
            static final XContentBuilderString _NESTED_OFFSET = new XContentBuilderString("offset");

        }
    }

}