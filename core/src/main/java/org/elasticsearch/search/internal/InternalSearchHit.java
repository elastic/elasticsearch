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

import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.lucene.Lucene.readExplanation;
import static org.elasticsearch.common.lucene.Lucene.writeExplanation;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseStoredFieldsValue;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.elasticsearch.search.fetch.subphase.highlight.HighlightField.readHighlightField;
import static org.elasticsearch.search.internal.InternalSearchHitField.readSearchHitField;

public class InternalSearchHit implements SearchHit {

    private transient int docId;

    private static final float DEFAULT_SCORE = Float.NEGATIVE_INFINITY;
    private float score = DEFAULT_SCORE;

    private Text id;
    private Text type;

    private InternalNestedIdentity nestedIdentity;

    private long version = -1;

    private BytesReference source;

    private Map<String, SearchHitField> fields = emptyMap();

    private Map<String, HighlightField> highlightFields = null;

    private SearchSortValues sortValues = SearchSortValues.EMPTY;

    private String[] matchedQueries = Strings.EMPTY_ARRAY;

    private Explanation explanation;

    @Nullable
    private SearchShardTarget shard;

    private transient String index;

    private Map<String, Object> sourceAsMap;
    private byte[] sourceAsBytes;

    private Map<String, InternalSearchHits> innerHits;

    private InternalSearchHit() {

    }

    public InternalSearchHit(int docId) {
        this(docId, null, null, null);
    }

    public InternalSearchHit(int docId, String id, Text type, Map<String, SearchHitField> fields) {
        this(docId, id, type, null, fields);
    }

    public InternalSearchHit(int nestedTopDocId, String id, Text type, InternalNestedIdentity nestedIdentity, Map<String, SearchHitField> fields) {
        this.docId = nestedTopDocId;
        if (id != null) {
            this.id = new Text(id);
        } else {
            this.id = null;
        }
        this.type = type;
        this.nestedIdentity = nestedIdentity;
        this.fields = fields;
    }

    public int docId() {
        return this.docId;
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
        return this.index;
    }

    @Override
    public String getIndex() {
        return index();
    }

    @Override
    public String id() {
        return id != null ? id.string() : null;
    }

    @Override
    public String getId() {
        return id();
    }

    @Override
    public String type() {
        return type != null ? type.string() : null;
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
        if (this.source == null) {
            return null;
        }

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

    @Override
    public byte[] source() {
        if (source == null) {
            return null;
        }
        if (sourceAsBytes != null) {
            return sourceAsBytes;
        }
        this.sourceAsBytes = BytesReference.toBytes(sourceRef());
        return this.sourceAsBytes;
    }

    @Override
    public boolean hasSource() {
        return source != null;
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
        return fields == null ? emptyMap() : fields;
    }

    // returns the fields without handling null cases
    public Map<String, SearchHitField> fieldsOrNull() {
        return fields;
    }

    @Override
    public Map<String, SearchHitField> getFields() {
        return fields();
    }

    public void fields(Map<String, SearchHitField> fields) {
        this.fields = fields;
    }

    @Override
    public Map<String, HighlightField> highlightFields() {
        return highlightFields == null ? emptyMap() : highlightFields;
    }

    @Override
    public Map<String, HighlightField> getHighlightFields() {
        return highlightFields();
    }

    public void highlightFields(Map<String, HighlightField> highlightFields) {
        this.highlightFields = highlightFields;
    }

    public void sortValues(Object[] sortValues, DocValueFormat[] sortValueFormats) {
        sortValues(new SearchSortValues(sortValues, sortValueFormats));
    }

    public void sortValues(SearchSortValues sortValues) {
        this.sortValues = sortValues;
    }

    @Override
    public Object[] sortValues() {
        return sortValues.sortValues();
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
        if (target != null) {
            this.index = target.getIndex();
        }
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
        public static final String _INDEX = "_index";
        public static final String _TYPE = "_type";
        public static final String _ID = "_id";
        public static final String _VERSION = "_version";
        public static final String _SCORE = "_score";
        public static final String FIELDS = "fields";
        public static final String HIGHLIGHT = "highlight";
        public static final String SORT = "sort";
        public static final String MATCHED_QUERIES = "matched_queries";
        public static final String _EXPLANATION = "_explanation";
        public static final String VALUE = "value";
        public static final String DESCRIPTION = "description";
        public static final String DETAILS = "details";
        public static final String INNER_HITS = "inner_hits";
        public static final String _SHARD = "_shard";
        public static final String _NODE = "_node";
    }

    // public because we render hit as part of completion suggestion option
    public XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        List<SearchHitField> metaFields = new ArrayList<>();
        List<SearchHitField> otherFields = new ArrayList<>();
        if (fields != null && !fields.isEmpty()) {
            for (SearchHitField field : fields.values()) {
                if (field.values().isEmpty()) {
                    continue;
                }
                if (field.isMetadataField()) {
                    metaFields.add(field);
                } else {
                    otherFields.add(field);
                }
            }
        }

        // For inner_hit hits shard is null and that is ok, because the parent search hit has all this information.
        // Even if this was included in the inner_hit hits this would be the same, so better leave it out.
        if (explanation() != null && shard != null) {
            builder.field(Fields._SHARD, shard.getShardId());
            builder.field(Fields._NODE, shard.getNodeIdText());
        }
        if (nestedIdentity != null) {
            nestedIdentity.toXContent(builder, params);
        } else {
            if (index != null) {
                builder.field(Fields._INDEX, index);
            }
            if (type != null) {
                builder.field(Fields._TYPE, type);
            }
            if (id != null) {
                builder.field(Fields._ID, id);
            }
        }
        if (version != -1) {
            builder.field(Fields._VERSION, version);
        }
        if (Float.isNaN(score)) {
            builder.nullField(Fields._SCORE);
        } else {
            builder.field(Fields._SCORE, score);
        }
        for (SearchHitField field : metaFields) {
            Object value = field.value();
            builder.field(field.name(), value);
        }
        if (source != null) {
            XContentHelper.writeRawField(SourceFieldMapper.NAME, source, builder, params);
        }
        if (!otherFields.isEmpty()) {
            builder.startObject(Fields.FIELDS);
            for (SearchHitField field : otherFields) {
                builder.startArray(field.name());
                for (Object value : field.getValues()) {
                    builder.value(value);
                }
                builder.endArray();
            }
            builder.endObject();
        }
        if (highlightFields != null && !highlightFields.isEmpty()) {
            builder.startObject(Fields.HIGHLIGHT);
            for (HighlightField field : highlightFields.values()) {
                field.toXContent(builder, params);
            }
            builder.endObject();
        }
        sortValues.toXContent(builder, params);
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
        return builder;
    }

    private static ObjectParser<Map<String, Object>, Void> PARSER = new ObjectParser<>("innerHitsParser", HashMap<String, Object>::new);

    static {
        declareInnerHitsParseFields(PARSER);
    }

    public static void declareInnerHitsParseFields(ObjectParser<Map<String, Object>, Void> parser) {
        parser.declareString((map, value) -> map.put(Fields._TYPE, value), new ParseField(Fields._TYPE));
        parser.declareString((map, value) -> map.put(Fields._INDEX, value), new ParseField(Fields._INDEX));
        parser.declareString((map, value) -> map.put(Fields._ID, value), new ParseField(Fields._ID));
        parser.declareString((map, value) -> map.put(Fields._NODE, value), new ParseField(Fields._NODE));
        parser.declareField((map, value) -> map.put(Fields._SCORE, value),
                (p, c) -> {
                    if (p.currentToken() == XContentParser.Token.VALUE_NUMBER || p.currentToken() == XContentParser.Token.VALUE_STRING) {
                        return p.floatValue();
                    } else {
                        return Float.NaN;
                    }
                }
                , new ParseField(Fields._SCORE), ValueType.FLOAT_OR_NULL);
        parser.declareLong((map, value) -> map.put(Fields._VERSION, value), new ParseField(Fields._VERSION));
        parser.declareField((map, value) -> map.put(Fields._SHARD, value), (p, c) -> ShardId.fromString(p.text()),
                new ParseField(Fields._SHARD), ValueType.STRING);
        // we need to declare all possible metadata field individually
        for (String metadatafield : MapperService.getAllMetaFields()) {
            if (metadatafield.equals(Fields._ID) == false && metadatafield.equals(Fields._INDEX) == false
                    && metadatafield.equals(Fields._TYPE) == false) {
                parser.declareField((map, field) -> {
                    Object fields = map.get(Fields.FIELDS);
                    if (fields == null) {
                        fields = new HashMap<String, SearchHitField>();
                        map.put(Fields.FIELDS, fields);
                    }
                    ((Map<String, SearchHitField>) fields).put(field.name(), field);
                }, (p, c) -> {
                    List<Object> values = new ArrayList<>();
                    values.add(parseStoredFieldsValue(p));
                    return (SearchHitField) new InternalSearchHitField(metadatafield, values);
                }, new ParseField(metadatafield), ValueType.VALUE);
            }
        }
        parser.declareObject((map, value) -> map.put(SourceFieldMapper.NAME, value),
                (p, c) -> {
                    try (XContentBuilder builder = XContentBuilder.builder(p.contentType().xContent())) {
                        // the original document gets slightly modified: whitespaces or pretty printing are not preserved,
                        // it all depends on the current builder settings
                        builder.copyCurrentStructure(p);
                        return builder.bytes();
                    }
                }, new ParseField(SourceFieldMapper.NAME));
        parser.declareObject((map, value) -> map.put(Fields.HIGHLIGHT, value), (p,c) -> parseHighlightFields(p), new ParseField(Fields.HIGHLIGHT));
        parser.declareObject((map, value) -> {
            Object fields = map.get(Fields.FIELDS);
            if (fields == null) {
                map.put(Fields.FIELDS, value);
            } else {
                ((Map<String, SearchHitField>) fields).putAll(value);
            }
        }, (p, c) -> parseFields(p), new ParseField(Fields.FIELDS));
        parser.declareObject((map, value) -> map.put(Fields._EXPLANATION, value),
                (p, c) -> parseExplanation(p), new ParseField(Fields._EXPLANATION));
        parser.declareObject((map, value) -> map.put(InternalNestedIdentity.Fields._NESTED, value),
                (p, c) -> InternalNestedIdentity.fromXContent(p), new ParseField(InternalNestedIdentity.Fields._NESTED));
        parser.declareObject((map, value) -> map.put(Fields.INNER_HITS, value), (p, c) -> parseInnerHits(p),
                new ParseField(Fields.INNER_HITS));
        parser.declareStringArray((map, list) -> map.put(Fields.MATCHED_QUERIES, list), new ParseField(Fields.MATCHED_QUERIES));
        parser.declareField((map, list) -> map.put(Fields.SORT, list), (p, c) -> SearchSortValues.fromXContent(p),
                new ParseField(Fields.SORT), ValueType.OBJECT_ARRAY);
    }

    public static InternalSearchHit fromXContent(XContentParser parser) {
        return createFromMap(PARSER.apply(parser, null));
    }

    public static InternalSearchHit createFromMap(Map<String, Object> values) {
        String type = get(Fields._TYPE, values, null);
        String id = get(Fields._ID, values, null);
        String index = get(Fields._INDEX, values, null);
        Float score = get(Fields._SCORE, values, DEFAULT_SCORE);
        Long version = get(Fields._VERSION, values, -1L);
        SearchSortValues sortValues = get(Fields.SORT, values, SearchSortValues.EMPTY);
        InternalNestedIdentity nestedIdentity = get(InternalNestedIdentity.Fields._NESTED, values, null);
        Map<String, HighlightField> highlightFields = get(Fields.HIGHLIGHT, values, null);
        BytesReference parsedSource = get(SourceFieldMapper.NAME, values, null);
        List<String> matchedQueries = get(Fields.MATCHED_QUERIES, values, null);
        Map<String, SearchHitField> fields = get(Fields.FIELDS, values, null);
        Explanation explanation = get(Fields._EXPLANATION, values, null);
        ShardId shardId = get(Fields._SHARD, values, null);
        String nodeId = get(Fields._NODE, values, null);
        Map<String, InternalSearchHits> innerHits = get(Fields.INNER_HITS, values, null);
        InternalSearchHit internalSearchHit = new InternalSearchHit(-1, id, new Text(type), nestedIdentity, Collections.emptyMap());
        internalSearchHit.index = index;
        internalSearchHit.score(score);
        internalSearchHit.version(version);
        internalSearchHit.sortValues(sortValues);
        internalSearchHit.highlightFields(highlightFields);
        internalSearchHit.sourceRef(parsedSource);
        internalSearchHit.explanation(explanation);
        internalSearchHit.setInnerHits(innerHits);
        if (matchedQueries != null) {
            internalSearchHit.matchedQueries(matchedQueries.toArray(new String[matchedQueries.size()]));
        }
        if (shardId != null && nodeId != null) {
            internalSearchHit.shard(new SearchShardTarget(nodeId, shardId));
        }
        internalSearchHit.fields(fields);
        return internalSearchHit;
    }

    private static <T> T get(String key, Map<String, Object> map, T defaultValue) {
        @SuppressWarnings("unchecked")
        T value = (T) map.get(key);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }

    private static Map<String, SearchHitField> parseFields(XContentParser parser) throws IOException {
        Map<String, SearchHitField> fields = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            List<Object> values = new ArrayList<>();
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser::getTokenLocation);
            while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                values.add(parseStoredFieldsValue(parser));
            }
            fields.put(fieldName, new InternalSearchHitField(fieldName, values));
        }
        return fields;
    }

    private static Map<String, InternalSearchHits> parseInnerHits(XContentParser parser) throws IOException {
        Map<String, InternalSearchHits> innerHits = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            // parse the key
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            String name = parser.currentName();
            innerHits.put(name, InternalSearchHits.fromXContent(parser));
            parser.nextToken();
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser::getTokenLocation);
        }
        return innerHits;
    }

    private static Map<String, HighlightField> parseHighlightFields(XContentParser parser) throws IOException {
        Map<String, HighlightField> highlightFields = new HashMap<>();
        while((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            HighlightField highlightField = HighlightField.fromXContent(parser);
            highlightFields.put(highlightField.getName(), highlightField);
        }
        return highlightFields;
    }

    private static Explanation parseExplanation(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        XContentParser.Token token;
        Float value = null;
        String description = null;
        List<Explanation> details = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, () -> parser.getTokenLocation());
            String currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (Fields.VALUE.equals(currentFieldName)) {
                value = parser.floatValue();
            } else if (Fields.DESCRIPTION.equals(currentFieldName)) {
                description = parser.textOrNull();
            } else if (Fields.DETAILS.equals(currentFieldName)) {
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, () -> parser.getTokenLocation());
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    details.add(parseExplanation(parser));
                }
            } else {
                throwUnknownField(currentFieldName, parser.getTokenLocation());
            }
        }
        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing explanation value");
        }
        if (description == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing explanation description");
        }
        return Explanation.match(value, description, details);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toInnerXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public static InternalSearchHit readSearchHit(StreamInput in) throws IOException {
        InternalSearchHit hit = new InternalSearchHit();
        hit.readFrom(in);
        return hit;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        score = in.readFloat();
        id = in.readOptionalText();
        type = in.readOptionalText();
        nestedIdentity = in.readOptionalWriteable(InternalNestedIdentity::new);
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
            fields = emptyMap();
        } else if (size == 1) {
            SearchHitField hitField = readSearchHitField(in);
            fields = singletonMap(hitField.name(), hitField);
        } else {
            Map<String, SearchHitField> fields = new HashMap<>();
            for (int i = 0; i < size; i++) {
                SearchHitField hitField = readSearchHitField(in);
                fields.put(hitField.name(), hitField);
            }
            this.fields = unmodifiableMap(fields);
        }

        size = in.readVInt();
        if (size == 0) {
            highlightFields = emptyMap();
        } else if (size == 1) {
            HighlightField field = readHighlightField(in);
            highlightFields = singletonMap(field.name(), field);
        } else {
            Map<String, HighlightField> highlightFields = new HashMap<>();
            for (int i = 0; i < size; i++) {
                HighlightField field = readHighlightField(in);
                highlightFields.put(field.name(), field);
            }
            this.highlightFields = unmodifiableMap(highlightFields);
        }

        sortValues = new SearchSortValues(in);

        size = in.readVInt();
        if (size > 0) {
            matchedQueries = new String[size];
            for (int i = 0; i < size; i++) {
                matchedQueries[i] = in.readString();
            }
        }
        // we call the setter here because that also sets the local index parameter
        shard(in.readOptionalWriteable(SearchShardTarget::new));
        size = in.readVInt();
        if (size > 0) {
            innerHits = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                InternalSearchHits value = InternalSearchHits.readSearchHits(in);
                innerHits.put(key, value);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(score);
        out.writeOptionalText(id);
        out.writeOptionalText(type);
        out.writeOptionalWriteable(nestedIdentity);
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
        sortValues.writeTo(out);

        if (matchedQueries.length == 0) {
            out.writeVInt(0);
        } else {
            out.writeVInt(matchedQueries.length);
            for (String matchedFilter : matchedQueries) {
                out.writeString(matchedFilter);
            }
        }
        out.writeOptionalWriteable(shard);
        if (innerHits == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(innerHits.size());
            for (Map.Entry<String, InternalSearchHits> entry : innerHits.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        InternalSearchHit other = (InternalSearchHit) obj;
        return Objects.equals(id, other.id)
                && Objects.equals(type, other.type)
                && Objects.equals(nestedIdentity, other.nestedIdentity)
                && Objects.equals(version, other.version)
                && Objects.equals(source, other.source)
                && Objects.equals(fields, other.fields)
                && Objects.equals(highlightFields(), other.highlightFields())
                && Arrays.equals(matchedQueries, other.matchedQueries)
                && Objects.equals(explanation, other.explanation)
                && Objects.equals(shard, other.shard)
                && Objects.equals(innerHits, other.innerHits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, nestedIdentity, version, source, fields, highlightFields(), Arrays.hashCode(matchedQueries),
                explanation, shard, innerHits);
    }

    public static final class InternalNestedIdentity implements NestedIdentity, Writeable, ToXContent {

        private Text field;
        private int offset;
        private InternalNestedIdentity child;

        public InternalNestedIdentity(String field, int offset, InternalNestedIdentity child) {
            this.field = new Text(field);
            this.offset = offset;
            this.child = child;
        }

        InternalNestedIdentity(StreamInput in) throws IOException {
            field = in.readOptionalText();
            offset = in.readInt();
            child = in.readOptionalWriteable(InternalNestedIdentity::new);
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
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalText(field);
            out.writeInt(offset);
            out.writeOptionalWriteable(child);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields._NESTED);
            return innerToXContent(builder, params);
        }

        /**
         * Rendering of the inner XContent object without the leading field name. This way the structure innerToXContent renders and
         * fromXContent parses correspond to each other.
         */
        XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
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

        private static final ConstructingObjectParser<InternalNestedIdentity, Void> PARSER = new ConstructingObjectParser<>(
                "nested_identity",
                ctorArgs -> new InternalNestedIdentity((String) ctorArgs[0], (int) ctorArgs[1], (InternalNestedIdentity) ctorArgs[2]));
        static {
            PARSER.declareString(constructorArg(), new ParseField(Fields._NESTED_FIELD));
            PARSER.declareInt(constructorArg(), new ParseField(Fields._NESTED_OFFSET));
            PARSER.declareObject(optionalConstructorArg(), PARSER, new ParseField(Fields._NESTED));
        }

        public static InternalNestedIdentity fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            InternalNestedIdentity other = (InternalNestedIdentity) obj;
            return Objects.equals(field, other.field) &&
                    Objects.equals(offset, other.offset) &&
                    Objects.equals(child, other.child);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, offset, child);
        }

        public static class Fields {
            public static final String _NESTED = "_nested";
            public static final String _NESTED_FIELD = "field";
            public static final String _NESTED_OFFSET = "offset";
        }
    }
}
