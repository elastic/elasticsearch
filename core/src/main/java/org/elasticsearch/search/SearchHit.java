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

package org.elasticsearch.search;

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
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
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
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;
import static org.elasticsearch.search.fetch.subphase.highlight.HighlightField.readHighlightField;

/**
 * A single search hit.
 *
 * @see SearchHits
 */
public class SearchHit implements Streamable, ToXContentObject, Iterable<SearchHitField> {

    private transient int docId;

    private static final float DEFAULT_SCORE = Float.NEGATIVE_INFINITY;
    private float score = DEFAULT_SCORE;

    private Text id;
    private Text type;

    private NestedIdentity nestedIdentity;

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

    private Map<String, SearchHits> innerHits;

    private SearchHit() {

    }

    public SearchHit(int docId) {
        this(docId, null, null, null);
    }

    public SearchHit(int docId, String id, Text type, Map<String, SearchHitField> fields) {
        this(docId, id, type, null, fields);
    }

    public SearchHit(int nestedTopDocId, String id, Text type, NestedIdentity nestedIdentity, Map<String, SearchHitField> fields) {
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

    /**
     * The score.
     * @deprecated  use {@link #getScore()} instead
     */
    @Deprecated
    public float score() {
        return this.score;
    }

    /**
     * The score.
     */
    public float getScore() {
        return score();
    }

    public void version(long version) {
        this.version = version;
    }

    /**
     * The version of the hit.
     * @deprecated use {@link #getVersion()} instead
     */
    @Deprecated
    public long version() {
        return this.version;
    }

    /**
     * The version of the hit.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * The index of the hit.
     * @deprecated use {@link #getIndex()} instead
     */
    @Deprecated
    public String index() {
        return getIndex();
    }

    /**
     * The index of the hit.
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * The id of the document.
     * @deprecated use {@link #getId()} instead
     */
    @Deprecated
    public String id() {
        return getId();
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return id != null ? id.string() : null;
    }

    /**
     * The type of the document.
     * @deprecated use {@link #getType()} instead
     */
    @Deprecated
    public String type() {
        return getType();
    }

    /**
     * The type of the document.
     */
    public String getType() {
        return type != null ? type.string() : null;
    }

    /**
     * If this is a nested hit then nested reference information is returned otherwise <code>null</code> is returned.
     */
    public NestedIdentity getNestedIdentity() {
        return nestedIdentity;
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     * @deprecated use {@link #getSourceRef()} instead
     */
    @Deprecated
    public BytesReference sourceRef() {
       return getSourceRef();
    }

    /**
     * Sets representation, might be compressed....
     */
    public SearchHit sourceRef(BytesReference source) {
        this.source = source;
        this.sourceAsBytes = null;
        this.sourceAsMap = null;
        return this;
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    public BytesReference getSourceRef() {
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
     * The source of the document (can be <tt>null</tt>). Note, its a copy of the source
     * into a byte array, consider using {@link #sourceRef()} so there won't be a need to copy.
     */
    @Deprecated
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

    /**
     * Is the source available or not. A source with no fields will return true. This will return false if {@code fields} doesn't contain
     * {@code _source} or if source is disabled in the mapping.
     */
    public boolean hasSource() {
        return source != null;
    }

    /**
     * The source of the document as a map (can be <tt>null</tt>).
     */
    public Map<String, Object> getSource() {
        return sourceAsMap();
    }

    /**
     * The source of the document as string (can be <tt>null</tt>).
     * @deprecated use {@link #getSourceAsString()} instead
     */
    @Deprecated
    public String sourceAsString() {
        return getSourceAsString();
    }

    /**
     * The source of the document as string (can be <tt>null</tt>).
     */
    public String getSourceAsString() {
        if (source == null) {
            return null;
        }
        try {
            return XContentHelper.convertToJson(sourceRef(), false);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to convert source to a json string");
        }
    }

    /**
     * The source of the document as a map (can be <tt>null</tt>).
     * @deprecated use {@link #getSourceAsMap()} instgead
     */
    @Deprecated
    public Map<String, Object> sourceAsMap() throws ElasticsearchParseException {
        return getSourceAsMap();
    }

    /**
     * The source of the document as a map (can be <tt>null</tt>).
     */
    public Map<String, Object> getSourceAsMap() throws ElasticsearchParseException {
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

    /**
     * The hit field matching the given field name.
     * @deprecated use {@link #getField(String)} instead
     */
    @Deprecated
    public SearchHitField field(String fieldName) {
        return getField(fieldName);
    }

    /**
     * The hit field matching the given field name.
     */
    public SearchHitField getField(String fieldName) {
        return fields().get(fieldName);
    }

    /**
     * A map of hit fields (from field name to hit fields) if additional fields
     * were required to be loaded.
     * @deprecated use {@link #getFields()} instead
     */
    @Deprecated
    public Map<String, SearchHitField> fields() {
        return getFields();
    }

    // returns the fields without handling null cases
    public Map<String, SearchHitField> fieldsOrNull() {
        return fields;
    }

    /**
     * A map of hit fields (from field name to hit fields) if additional fields
     * were required to be loaded.
     */
    public Map<String, SearchHitField> getFields() {
        return fields == null ? emptyMap() : fields;
    }

    public void fields(Map<String, SearchHitField> fields) {
        this.fields = fields;
    }

    /**
     * A map of highlighted fields.
     * @deprecated use {@link #getHighlightFields()} instead
     */
    @Deprecated
    public Map<String, HighlightField> highlightFields() {
        return highlightFields == null ? emptyMap() : highlightFields;
    }

    /**
     * A map of highlighted fields.
     */
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

    /**
     * An array of the sort values used.
     * @deprecated use {@link #getSortValues()} instead
     */
    @Deprecated
    public Object[] sortValues() {
        return sortValues.sortValues();
    }

    /**
     * An array of the sort values used.
     */
    public Object[] getSortValues() {
        return sortValues();
    }

    /**
     * If enabled, the explanation of the search hit.
     * @deprecated use {@link #getExplanation()} instead
     */
    @Deprecated
    public Explanation explanation() {
        return explanation;
    }

    /**
     * If enabled, the explanation of the search hit.
     */
    public Explanation getExplanation() {
        return explanation();
    }

    public void explanation(Explanation explanation) {
        this.explanation = explanation;
    }

    /**
     * The shard of the search hit.
     * @deprecated use {@link #getShard()} instead
     */
    @Deprecated
    public SearchShardTarget shard() {
        return shard;
    }

    /**
     * The shard of the search hit.
     */
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

    /**
     * The set of query and filter names the query matched with. Mainly makes sense for compound filters and queries.
     */
    @Deprecated
    public String[] matchedQueries() {
        return this.matchedQueries;
    }

    /**
     * The set of query and filter names the query matched with. Mainly makes sense for compound filters and queries.
     */
    public String[] getMatchedQueries() {
        return this.matchedQueries;
    }

    /**
     * @return Inner hits or <code>null</code> if there are none
     */
    @SuppressWarnings("unchecked")
    public Map<String, SearchHits> getInnerHits() {
        return (Map) innerHits;
    }

    public void setInnerHits(Map<String, SearchHits> innerHits) {
        this.innerHits = innerHits;
    }

    public static class Fields {
        static final String _INDEX = "_index";
        static final String _TYPE = "_type";
        static final String _ID = "_id";
        static final String _VERSION = "_version";
        static final String _SCORE = "_score";
        static final String FIELDS = "fields";
        static final String HIGHLIGHT = "highlight";
        static final String SORT = "sort";
        static final String MATCHED_QUERIES = "matched_queries";
        static final String _EXPLANATION = "_explanation";
        static final String VALUE = "value";
        static final String DESCRIPTION = "description";
        static final String DETAILS = "details";
        static final String INNER_HITS = "inner_hits";
        static final String _SHARD = "_shard";
        static final String _NODE = "_node";
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
            for (Map.Entry<String, SearchHits> entry : innerHits.entrySet()) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        return builder;
    }

    public static SearchHit fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        String currentFieldName = null;
        String type = null, id = null;
        String index = null;
        float score = DEFAULT_SCORE;
        long version = -1;
        SearchSortValues sortValues = SearchSortValues.EMPTY;
        NestedIdentity nestedIdentity = null;
        Map<String, HighlightField> highlightFields = new HashMap<>();
        BytesReference parsedSource = null;
        List<String> matchedQueries = new ArrayList<>();
        Map<String, SearchHitField> fields = new HashMap<>();
        Explanation explanation = null;
        ShardId shardId = null;
        String nodeId = null;
        Map<String, SearchHits> innerHits = null;
        while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (Fields._TYPE.equals(currentFieldName)) {
                    type = parser.text();
                } else if (Fields._INDEX.equals(currentFieldName)) {
                    index = parser.text();
                } else if (Fields._ID.equals(currentFieldName)) {
                    id = parser.text();
                } else if (Fields._SCORE.equals(currentFieldName)) {
                     score = parser.floatValue();
                } else if (Fields._VERSION.equals(currentFieldName)) {
                    version = parser.longValue();
                } else if (Fields._SHARD.equals(currentFieldName)) {
                    shardId = ShardId.fromString(parser.text());
                } else if (Fields._NODE.equals(currentFieldName)) {
                   nodeId = parser.text();
                } else if (MapperService.isMetadataField(currentFieldName)) {
                    List<Object> values = new ArrayList<>();
                    values.add(parseStoredFieldsValue(parser));
                    fields.put(currentFieldName, new SearchHitField(currentFieldName, values));
                } else {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_NULL) {
                if (Fields._SCORE.equals(currentFieldName)) {
                     score = Float.NaN;
                } else {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SourceFieldMapper.NAME.equals(currentFieldName)) {
                    try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                        //the original document gets slightly modified: whitespaces or pretty printing are not preserved,
                        //it all depends on the current builder settings
                        builder.copyCurrentStructure(parser);
                        parsedSource = builder.bytes();
                    }
                } else if (Fields.HIGHLIGHT.equals(currentFieldName)) {
                    while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        HighlightField highlightField = HighlightField.fromXContent(parser);
                        highlightFields.put(highlightField.getName(), highlightField);
                    }
                } else if (Fields.FIELDS.equals(currentFieldName)) {
                    while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        String fieldName = parser.currentName();
                        List<Object> values = new ArrayList<>();
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser::getTokenLocation);
                        while((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            values.add(parseStoredFieldsValue(parser));
                        }
                        fields.put(fieldName, new SearchHitField(fieldName, values));
                    }
                } else if (Fields._EXPLANATION.equals(currentFieldName)) {
                    explanation = parseExplanation(parser);
                } else if (Fields.INNER_HITS.equals(currentFieldName)) {
                    innerHits = new HashMap<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        // parse the key
                        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
                        String name = parser.currentName();
                        innerHits.put(name, SearchHits.fromXContent(parser));
                        parser.nextToken();
                        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser::getTokenLocation);
                    }
                } else if (NestedIdentity.Fields._NESTED.equals(currentFieldName)) {
                    nestedIdentity = NestedIdentity.fromXContent(parser);
                } else {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Fields.SORT.equals(currentFieldName)) {
                    sortValues = SearchSortValues.fromXContent(parser);
                } else if (Fields.MATCHED_QUERIES.equals(currentFieldName)) {
                    while((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        matchedQueries.add(parser.text());
                    }
                } else {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else {
                throwUnknownToken(token, parser.getTokenLocation());
            }
        }
        SearchHit searchHit = new SearchHit(-1, id, new Text(type), nestedIdentity, Collections.emptyMap());
        searchHit.index = index;
        searchHit.score(score);
        searchHit.version(version);
        searchHit.sortValues(sortValues);
        searchHit.highlightFields(highlightFields);
        searchHit.sourceRef(parsedSource);
        searchHit.explanation(explanation);
        searchHit.setInnerHits(innerHits);
        if (matchedQueries.size() > 0) {
            searchHit.matchedQueries(matchedQueries.toArray(new String[matchedQueries.size()]));
        }
        if (shardId != null && nodeId != null) {
            searchHit.shard(new SearchShardTarget(nodeId, shardId));
        }
        searchHit.fields(fields);
        return searchHit;
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

    public static SearchHit readSearchHit(StreamInput in) throws IOException {
        SearchHit hit = new SearchHit();
        hit.readFrom(in);
        return hit;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        score = in.readFloat();
        id = in.readOptionalText();
        type = in.readOptionalText();
        nestedIdentity = in.readOptionalWriteable(NestedIdentity::new);
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
            SearchHitField hitField = SearchHitField.readSearchHitField(in);
            fields = singletonMap(hitField.name(), hitField);
        } else {
            Map<String, SearchHitField> fields = new HashMap<>();
            for (int i = 0; i < size; i++) {
                SearchHitField hitField = SearchHitField.readSearchHitField(in);
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
                SearchHits value = SearchHits.readSearchHits(in);
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
            for (Map.Entry<String, SearchHits> entry : innerHits.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    /**
     * Encapsulates the nested identity of a hit.
     */
    public static final class NestedIdentity implements Writeable, ToXContent {

        private Text field;
        private int offset;
        private NestedIdentity child;

        public NestedIdentity(String field, int offset, NestedIdentity child) {
            this.field = new Text(field);
            this.offset = offset;
            this.child = child;
        }

        NestedIdentity(StreamInput in) throws IOException {
            field = in.readOptionalText();
            offset = in.readInt();
            child = in.readOptionalWriteable(NestedIdentity::new);
        }

        /**
         * Returns the nested field in the source this hit originates from
         */
        public Text getField() {
            return field;
        }

        /**
         * Returns the offset in the nested array of objects in the source this hit
         */
        public int getOffset() {
            return offset;
        }

        /**
         * Returns the next child nested level if there is any, otherwise <code>null</code> is returned.
         *
         * In the case of mappings with multiple levels of nested object fields
         */
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

        private static final ConstructingObjectParser<NestedIdentity, Void> PARSER = new ConstructingObjectParser<>(
                "nested_identity",
                ctorArgs -> new NestedIdentity((String) ctorArgs[0], (int) ctorArgs[1], (NestedIdentity) ctorArgs[2]));
        static {
            PARSER.declareString(constructorArg(), new ParseField(Fields._NESTED_FIELD));
            PARSER.declareInt(constructorArg(), new ParseField(Fields._NESTED_OFFSET));
            PARSER.declareObject(optionalConstructorArg(), PARSER, new ParseField(Fields._NESTED));
        }

        public static NestedIdentity fromXContent(XContentParser parser) {
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
            NestedIdentity other = (NestedIdentity) obj;
            return Objects.equals(field, other.field) &&
                    Objects.equals(offset, other.offset) &&
                    Objects.equals(child, other.child);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, offset, child);
        }

        public static class Fields {
            static final String _NESTED = "_nested";
            static final String _NESTED_FIELD = "field";
            static final String _NESTED_OFFSET = "offset";
        }
    }
}
