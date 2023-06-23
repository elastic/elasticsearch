/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.fetch.subphase.LookupField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.lucene.Lucene.readExplanation;
import static org.elasticsearch.common.lucene.Lucene.writeExplanation;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A single search hit.
 *
 * @see SearchHits
 */
public final class SearchHit implements Writeable, ToXContentObject, Iterable<DocumentField> {

    private final transient int docId;

    private static final float DEFAULT_SCORE = Float.NaN;
    private float score = DEFAULT_SCORE;

    private static final int NO_RANK = -1;
    private int rank = NO_RANK;

    private final Text id;

    private final NestedIdentity nestedIdentity;

    private long version = -1;
    private long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

    private BytesReference source;

    private final Map<String, DocumentField> documentFields = new HashMap<>();
    private final Map<String, DocumentField> metaFields = new HashMap<>();

    private Map<String, HighlightField> highlightFields = null;

    private SearchSortValues sortValues = SearchSortValues.EMPTY;

    private Map<String, Float> matchedQueries = Collections.emptyMap();

    private Explanation explanation;

    @Nullable
    private SearchShardTarget shard;

    // These two fields normally get set when setting the shard target, so they hold the same values as the target thus don't get
    // serialized over the wire. When parsing hits back from xcontent though, in most of the cases (whenever explanation is disabled)
    // we can't rebuild the shard target object so we need to set these manually for users retrieval.
    private transient String index;
    private transient String clusterAlias;

    private Map<String, Object> sourceAsMap;

    private Map<String, SearchHits> innerHits;

    // used only in tests
    public SearchHit(int docId) {
        this(docId, null);
    }

    public SearchHit(int docId, String id) {
        this(docId, id, null);
    }

    public SearchHit(int nestedTopDocId, String id, NestedIdentity nestedIdentity) {
        this.docId = nestedTopDocId;
        if (id != null) {
            this.id = new Text(id);
        } else {
            this.id = null;
        }
        this.nestedIdentity = nestedIdentity;
    }

    public SearchHit(StreamInput in) throws IOException {
        docId = -1;
        score = in.readFloat();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            rank = in.readVInt();
        }
        id = in.readOptionalText();
        if (in.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            in.readOptionalText();
        }
        nestedIdentity = in.readOptionalWriteable(NestedIdentity::new);
        version = in.readLong();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        source = in.readBytesReference();
        if (source.length() == 0) {
            source = null;
        }
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_8_0)) {
            documentFields.putAll(in.readMap(DocumentField::new));
            metaFields.putAll(in.readMap(DocumentField::new));
        } else {
            Map<String, DocumentField> fields = readFields(in);
            fields.forEach(
                (fieldName, docField) -> (MapperService.isMetadataFieldStatic(fieldName) ? metaFields : documentFields).put(
                    fieldName,
                    docField
                )
            );
        }

        int size = in.readVInt();
        if (size == 0) {
            highlightFields = emptyMap();
        } else if (size == 1) {
            HighlightField field = new HighlightField(in);
            highlightFields = singletonMap(field.name(), field);
        } else {
            Map<String, HighlightField> highlightFields = new HashMap<>();
            for (int i = 0; i < size; i++) {
                HighlightField field = new HighlightField(in);
                highlightFields.put(field.name(), field);
            }
            this.highlightFields = unmodifiableMap(highlightFields);
        }

        sortValues = new SearchSortValues(in);

        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            matchedQueries = in.readOrderedMap(StreamInput::readString, StreamInput::readFloat);
        } else {
            size = in.readVInt();
            matchedQueries = new LinkedHashMap<>(size);
            for (int i = 0; i < size; i++) {
                matchedQueries.put(in.readString(), Float.NaN);
            }
        }

        // we call the setter here because that also sets the local index parameter
        shard(in.readOptionalWriteable(SearchShardTarget::new));
        size = in.readVInt();
        if (size > 0) {
            innerHits = Maps.newMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                SearchHits value = new SearchHits(in);
                innerHits.put(key, value);
            }
        } else {
            innerHits = null;
        }
    }

    private static final Text SINGLE_MAPPING_TYPE = new Text(MapperService.SINGLE_MAPPING_NAME);

    private static Map<String, DocumentField> readFields(StreamInput in) throws IOException {
        Map<String, DocumentField> fields;
        int size = in.readVInt();
        if (size == 0) {
            fields = emptyMap();
        } else if (size == 1) {
            DocumentField hitField = new DocumentField(in);
            fields = singletonMap(hitField.getName(), hitField);
        } else {
            fields = Maps.newMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                DocumentField field = new DocumentField(in);
                fields.put(field.getName(), field);
            }
            fields = unmodifiableMap(fields);
        }
        return fields;
    }

    private static void writeFields(StreamOutput out, Map<String, DocumentField> fields) throws IOException {
        if (fields == null) {
            out.writeVInt(0);
        } else {
            out.writeCollection(fields.values());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(score);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeVInt(rank);
        } else if (rank != NO_RANK) {
            throw new IllegalArgumentException("cannot serialize [rank] to version [" + out.getTransportVersion() + "]");
        }
        out.writeOptionalText(id);
        if (out.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            out.writeOptionalText(SINGLE_MAPPING_TYPE);
        }
        out.writeOptionalWriteable(nestedIdentity);
        out.writeLong(version);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeBytesReference(source);
        if (explanation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeExplanation(out, explanation);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_8_0)) {
            out.writeMap(documentFields, StreamOutput::writeString, (stream, documentField) -> documentField.writeTo(stream));
            out.writeMap(metaFields, StreamOutput::writeString, (stream, documentField) -> documentField.writeTo(stream));
        } else {
            writeFields(out, this.getFields());
        }
        if (highlightFields == null) {
            out.writeVInt(0);
        } else {
            out.writeCollection(highlightFields.values());
        }
        sortValues.writeTo(out);

        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeMap(matchedQueries, StreamOutput::writeString, StreamOutput::writeFloat);
        } else {
            out.writeStringArray(matchedQueries.keySet().toArray(new String[0]));
        }
        out.writeOptionalWriteable(shard);
        if (innerHits == null) {
            out.writeVInt(0);
        } else {
            out.writeMap(innerHits, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        }
    }

    public int docId() {
        return this.docId;
    }

    public void score(float score) {
        this.score = score;
    }

    /**
     * The score.
     */
    public float getScore() {
        return this.score;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public int getRank() {
        return this.rank;
    }

    public void version(long version) {
        this.version = version;
    }

    /**
     * The version of the hit.
     */
    public long getVersion() {
        return this.version;
    }

    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    public void setPrimaryTerm(long primaryTerm) {
        this.primaryTerm = primaryTerm;
    }

    /**
     * returns the sequence number of the last modification to the document, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
     * if not requested.
     **/
    public long getSeqNo() {
        return this.seqNo;
    }

    /**
     * returns the primary term of the last modification to the document, or {@link SequenceNumbers#UNASSIGNED_PRIMARY_TERM}
     * if not requested. */
    public long getPrimaryTerm() {
        return this.primaryTerm;
    }

    /**
     * The index of the hit.
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return id != null ? id.string() : null;
    }

    /**
     * If this is a nested hit then nested reference information is returned otherwise <code>null</code> is returned.
     */
    public NestedIdentity getNestedIdentity() {
        return nestedIdentity;
    }

    /**
     * Returns bytes reference, also uncompress the source if needed.
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
     * Sets representation, might be compressed....
     */
    public SearchHit sourceRef(BytesReference source) {
        this.source = source;
        this.sourceAsMap = null;
        return this;
    }

    /**
     * Is the source available or not. A source with no fields will return true. This will return false if {@code fields} doesn't contain
     * {@code _source} or if source is disabled in the mapping.
     */
    public boolean hasSource() {
        return source != null;
    }

    /**
     * The source of the document as string (can be {@code null}).
     */
    public String getSourceAsString() {
        if (source == null) {
            return null;
        }
        try {
            return XContentHelper.convertToJson(getSourceRef(), false);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to convert source to a json string");
        }
    }

    /**
     * The source of the document as a map (can be {@code null}).
     */
    public Map<String, Object> getSourceAsMap() {
        if (source == null) {
            return null;
        }
        if (sourceAsMap != null) {
            return sourceAsMap;
        }

        sourceAsMap = Source.fromBytes(source).source();
        return sourceAsMap;
    }

    @Override
    public Iterator<DocumentField> iterator() {
        // need to join the fields and metadata fields
        Map<String, DocumentField> allFields = this.getFields();
        return allFields.values().iterator();
    }

    /**
     * The hit field matching the given field name.
     */
    public DocumentField field(String fieldName) {
        DocumentField result = documentFields.get(fieldName);
        if (result != null) {
            return result;
        } else {
            return metaFields.get(fieldName);
        }
    }

    /*
    * Adds a new DocumentField to the map in case both parameters are not null.
    * */
    public void setDocumentField(String fieldName, DocumentField field) {
        if (fieldName == null || field == null) return;
        this.documentFields.put(fieldName, field);
    }

    public void addDocumentFields(Map<String, DocumentField> docFields, Map<String, DocumentField> metaFields) {
        this.documentFields.putAll(docFields);
        this.metaFields.putAll(metaFields);
    }

    /**
     * @return a map of metadata fields for this hit
     */
    public Map<String, DocumentField> getMetadataFields() {
        return Collections.unmodifiableMap(metaFields);
    }

    /**
     * @return a map of non-metadata fields requested for this hit
     */
    public Map<String, DocumentField> getDocumentFields() {
        return Collections.unmodifiableMap(documentFields);
    }

    /**
     * A map of hit fields (from field name to hit fields) if additional fields
     * were required to be loaded. Includes both document and metadata fields.
     */
    public Map<String, DocumentField> getFields() {
        if (metaFields.size() > 0 || documentFields.size() > 0) {
            final Map<String, DocumentField> fields = new HashMap<>();
            fields.putAll(metaFields);
            fields.putAll(documentFields);
            return fields;
        } else {
            return emptyMap();
        }
    }

    /**
     * Whether this search hit has any lookup fields
     */
    public boolean hasLookupFields() {
        return getDocumentFields().values().stream().anyMatch(doc -> doc.getLookupFields().isEmpty() == false);
    }

    /**
     * Resolve the lookup fields with the given results and merge them as regular fetch fields.
     */
    public void resolveLookupFields(Map<LookupField, List<Object>> lookupResults) {
        if (lookupResults.isEmpty()) {
            return;
        }
        final List<String> fields = new ArrayList<>(documentFields.keySet());
        for (String field : fields) {
            documentFields.computeIfPresent(field, (k, docField) -> {
                if (docField.getLookupFields().isEmpty()) {
                    return docField;
                }
                final List<Object> newValues = new ArrayList<>(docField.getValues());
                for (LookupField lookupField : docField.getLookupFields()) {
                    final List<Object> resolvedValues = lookupResults.get(lookupField);
                    if (resolvedValues != null) {
                        newValues.addAll(resolvedValues);
                    }
                }
                if (newValues.isEmpty() && docField.getIgnoredValues().isEmpty()) {
                    return null;
                } else {
                    return new DocumentField(docField.getName(), newValues, docField.getIgnoredValues());
                }
            });
        }
        assert hasLookupFields() == false : "Some lookup fields are not resolved";
    }

    /**
     * A map of highlighted fields.
     */
    public Map<String, HighlightField> getHighlightFields() {
        return highlightFields == null ? emptyMap() : highlightFields;
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
     * An array of the (formatted) sort values used.
     */
    public Object[] getSortValues() {
        return sortValues.getFormattedSortValues();
    }

    /**
     * An array of the (raw) sort values used.
     */
    public Object[] getRawSortValues() {
        return sortValues.getRawSortValues();
    }

    /**
     * If enabled, the explanation of the search hit.
     */
    public Explanation getExplanation() {
        return explanation;
    }

    public void explanation(Explanation explanation) {
        this.explanation = explanation;
    }

    /**
     * The shard of the search hit.
     */
    public SearchShardTarget getShard() {
        return shard;
    }

    public void shard(SearchShardTarget target) {
        if (innerHits != null) {
            for (SearchHits innerHits : innerHits.values()) {
                for (SearchHit innerHit : innerHits) {
                    innerHit.shard(target);
                }
            }
        }

        this.shard = target;
        if (target != null) {
            this.index = target.getIndex();
            this.clusterAlias = target.getClusterAlias();
        }
    }

    /**
     * Returns the cluster alias this hit comes from or null if it comes from a local cluster
     */
    public String getClusterAlias() {
        return clusterAlias;
    }

    public void matchedQueries(Map<String, Float> matchedQueries) {
        this.matchedQueries = matchedQueries;
    }

    /**
     * The set of query and filter names the query matched with. Mainly makes sense for compound filters and queries.
     */
    public String[] getMatchedQueries() {
        return matchedQueries == null ? new String[0] : matchedQueries.keySet().toArray(new String[0]);
    }

    /**
     * @return The score of the provided named query if it matches, {@code null} otherwise.
     */
    public Float getMatchedQueryScore(String name) {
        return getMatchedQueriesAndScores().get(name);
    }

    /**
     * @return The map of the named queries that matched and their associated score.
     */
    public Map<String, Float> getMatchedQueriesAndScores() {
        return matchedQueries == null ? Collections.emptyMap() : matchedQueries;
    }

    /**
     * @return Inner hits or <code>null</code> if there are none
     */
    public Map<String, SearchHits> getInnerHits() {
        return innerHits;
    }

    public void setInnerHits(Map<String, SearchHits> innerHits) {
        this.innerHits = innerHits;
    }

    public static class Fields {
        static final String _INDEX = "_index";
        static final String _ID = "_id";
        static final String _VERSION = "_version";
        static final String _SEQ_NO = "_seq_no";
        static final String _PRIMARY_TERM = "_primary_term";
        static final String _SCORE = "_score";
        static final String _RANK = "_rank";
        static final String FIELDS = "fields";
        static final String IGNORED_FIELD_VALUES = "ignored_field_values";
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

    // Following are the keys for storing the metadata fields and regular fields in the aggregation map.
    // These do not influence the structure of json serialization: document fields are still stored
    // under FIELDS and metadata are still scattered at the root level.
    static final String DOCUMENT_FIELDS = "document_fields";
    static final String METADATA_FIELDS = "metadata_fields";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toInnerXContent(builder, params);
        builder.endObject();
        return builder;
    }

    // public because we render hit as part of completion suggestion option
    public XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        // For inner_hit hits shard is null and that is ok, because the parent search hit has all this information.
        // Even if this was included in the inner_hit hits this would be the same, so better leave it out.
        if (getExplanation() != null && shard != null) {
            builder.field(Fields._SHARD, shard.getShardId());
            builder.field(Fields._NODE, shard.getNodeIdText());
        }
        if (index != null) {
            builder.field(Fields._INDEX, RemoteClusterAware.buildRemoteIndexName(clusterAlias, index));
        }
        if (builder.getRestApiVersion() == RestApiVersion.V_7 && metaFields.containsKey(MapperService.TYPE_FIELD_NAME) == false) {
            builder.field(MapperService.TYPE_FIELD_NAME, MapperService.SINGLE_MAPPING_NAME);
        }
        if (id != null) {
            builder.field(Fields._ID, id);
        }
        if (nestedIdentity != null) {
            nestedIdentity.toXContent(builder, params);
        }
        if (version != -1) {
            builder.field(Fields._VERSION, version);
        }

        if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            builder.field(Fields._SEQ_NO, seqNo);
            builder.field(Fields._PRIMARY_TERM, primaryTerm);
        }

        if (Float.isNaN(score)) {
            builder.nullField(Fields._SCORE);
        } else {
            builder.field(Fields._SCORE, score);
        }

        if (rank != NO_RANK) {
            builder.field(Fields._RANK, rank);
        }

        for (DocumentField field : metaFields.values()) {
            // ignore empty metadata fields
            if (field.getValues().size() == 0) {
                continue;
            }
            // _ignored is the only multi-valued meta field
            // TODO: can we avoid having an exception here?
            if (field.getName().equals(IgnoredFieldMapper.NAME)) {
                builder.field(field.getName(), field.getValues());
            } else {
                builder.field(field.getName(), field.<Object>getValue());
            }
        }
        if (source != null) {
            XContentHelper.writeRawField(SourceFieldMapper.NAME, source, builder, params);
        }
        if (documentFields.isEmpty() == false &&
        // ignore fields all together if they are all empty
            documentFields.values().stream().anyMatch(df -> df.getValues().size() > 0)) {
            builder.startObject(Fields.FIELDS);
            for (DocumentField field : documentFields.values()) {
                if (field.getValues().size() > 0) {
                    field.getValidValuesWriter().toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        // ignored field values
        if (documentFields.isEmpty() == false &&
        // omit ignored_field_values all together if there are none
            documentFields.values().stream().anyMatch(df -> df.getIgnoredValues().size() > 0)) {
            builder.startObject(Fields.IGNORED_FIELD_VALUES);
            for (DocumentField field : documentFields.values()) {
                if (field.getIgnoredValues().size() > 0) {
                    field.getIgnoredValuesWriter().toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        if (highlightFields != null && highlightFields.isEmpty() == false) {
            builder.startObject(Fields.HIGHLIGHT);
            for (HighlightField field : highlightFields.values()) {
                field.toXContent(builder, params);
            }
            builder.endObject();
        }
        sortValues.toXContent(builder, params);
        if (matchedQueries != null && matchedQueries.size() > 0) {
            boolean includeMatchedQueriesScore = params.paramAsBoolean(RestSearchAction.INCLUDE_NAMED_QUERIES_SCORE_PARAM, false);
            if (includeMatchedQueriesScore) {
                builder.startObject(Fields.MATCHED_QUERIES);
                for (Map.Entry<String, Float> entry : matchedQueries.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            } else {
                builder.startArray(Fields.MATCHED_QUERIES);
                for (String matchedFilter : matchedQueries.keySet()) {
                    builder.value(matchedFilter);
                }
                builder.endArray();
            }
        }
        if (getExplanation() != null) {
            builder.field(Fields._EXPLANATION);
            buildExplanation(builder, getExplanation());
        }
        if (innerHits != null) {
            builder.startObject(Fields.INNER_HITS);
            for (Map.Entry<String, SearchHits> entry : innerHits.entrySet()) {
                builder.startObject(entry.getKey());
                ChunkedToXContent.wrapAsToXContent(entry.getValue()).toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        return builder;
    }

    // All fields on the root level of the parsed SearhHit are interpreted as metadata fields
    // public because we use it in a completion suggestion option
    @SuppressWarnings("unchecked")
    public static final ObjectParser.UnknownFieldConsumer<Map<String, Object>> unknownMetaFieldConsumer = (map, fieldName, fieldValue) -> {
        Map<String, DocumentField> fieldMap = (Map<String, DocumentField>) map.computeIfAbsent(
            METADATA_FIELDS,
            v -> new HashMap<String, DocumentField>()
        );
        if (fieldName.equals(IgnoredFieldMapper.NAME)) {
            fieldMap.put(fieldName, new DocumentField(fieldName, (List<Object>) fieldValue));
        } else {
            fieldMap.put(fieldName, new DocumentField(fieldName, Collections.singletonList(fieldValue)));
        }
    };

    /**
     * This parser outputs a temporary map of the objects needed to create the
     * SearchHit instead of directly creating the SearchHit. The reason for this
     * is that this way we can reuse the parser when parsing xContent from
     * {@link org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option} which unfortunately inlines
     * the output of
     * {@link #toInnerXContent(XContentBuilder, org.elasticsearch.xcontent.ToXContent.Params)}
     * of the included search hit. The output of the map is used to create the
     * actual SearchHit instance via {@link #createFromMap(Map)}
     */
    private static final ObjectParser<Map<String, Object>, Void> MAP_PARSER = new ObjectParser<>(
        "innerHitParser",
        unknownMetaFieldConsumer,
        HashMap::new
    );

    static {
        declareInnerHitsParseFields(MAP_PARSER);
    }

    public static SearchHit fromXContent(XContentParser parser) {
        return createFromMap(MAP_PARSER.apply(parser, null));
    }

    public static void declareInnerHitsParseFields(ObjectParser<Map<String, Object>, Void> parser) {
        parser.declareString((map, value) -> map.put(Fields._INDEX, value), new ParseField(Fields._INDEX));
        parser.declareString((map, value) -> map.put(Fields._ID, value), new ParseField(Fields._ID));
        parser.declareString((map, value) -> map.put(Fields._NODE, value), new ParseField(Fields._NODE));
        parser.declareField(
            (map, value) -> map.put(Fields._SCORE, value),
            SearchHit::parseScore,
            new ParseField(Fields._SCORE),
            ValueType.FLOAT_OR_NULL
        );
        parser.declareInt((map, value) -> map.put(Fields._RANK, value), new ParseField(Fields._RANK));

        parser.declareLong((map, value) -> map.put(Fields._VERSION, value), new ParseField(Fields._VERSION));
        parser.declareLong((map, value) -> map.put(Fields._SEQ_NO, value), new ParseField(Fields._SEQ_NO));
        parser.declareLong((map, value) -> map.put(Fields._PRIMARY_TERM, value), new ParseField(Fields._PRIMARY_TERM));
        parser.declareField(
            (map, value) -> map.put(Fields._SHARD, value),
            (p, c) -> ShardId.fromString(p.text()),
            new ParseField(Fields._SHARD),
            ValueType.STRING
        );
        parser.declareObject(
            (map, value) -> map.put(SourceFieldMapper.NAME, value),
            (p, c) -> parseSourceBytes(p),
            new ParseField(SourceFieldMapper.NAME)
        );
        parser.declareObject(
            (map, value) -> map.put(Fields.HIGHLIGHT, value),
            (p, c) -> parseHighlightFields(p),
            new ParseField(Fields.HIGHLIGHT)
        );
        parser.declareObject((map, value) -> {
            Map<String, DocumentField> fieldMap = get(Fields.FIELDS, map, new HashMap<String, DocumentField>());
            fieldMap.putAll(value);
            map.put(DOCUMENT_FIELDS, fieldMap);
        }, (p, c) -> parseFields(p), new ParseField(Fields.FIELDS));
        parser.declareObject(
            (map, value) -> map.put(Fields._EXPLANATION, value),
            (p, c) -> parseExplanation(p),
            new ParseField(Fields._EXPLANATION)
        );
        parser.declareObject(
            (map, value) -> map.put(NestedIdentity._NESTED, value),
            NestedIdentity::fromXContent,
            new ParseField(NestedIdentity._NESTED)
        );
        parser.declareObject(
            (map, value) -> map.put(Fields.INNER_HITS, value),
            (p, c) -> parseInnerHits(p),
            new ParseField(Fields.INNER_HITS)
        );

        parser.declareField((p, map, context) -> {
            XContentParser.Token token = p.currentToken();
            Map<String, Float> matchedQueries = new LinkedHashMap<>();
            if (token == XContentParser.Token.START_OBJECT) {
                String fieldName = null;
                while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = p.currentName();
                    } else if (token.isValue()) {
                        matchedQueries.put(fieldName, p.floatValue());
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                while (p.nextToken() != XContentParser.Token.END_ARRAY) {
                    matchedQueries.put(p.text(), Float.NaN);
                }
            }
            map.put(Fields.MATCHED_QUERIES, matchedQueries);
        }, new ParseField(Fields.MATCHED_QUERIES), ObjectParser.ValueType.OBJECT_ARRAY);

        parser.declareField(
            (map, list) -> map.put(Fields.SORT, list),
            SearchSortValues::fromXContent,
            new ParseField(Fields.SORT),
            ValueType.OBJECT_ARRAY
        );
    }

    public static SearchHit createFromMap(Map<String, Object> values) {
        String id = get(Fields._ID, values, null);
        NestedIdentity nestedIdentity = get(NestedIdentity._NESTED, values, null);
        Map<String, DocumentField> metaFields = get(METADATA_FIELDS, values, Collections.emptyMap());
        Map<String, DocumentField> documentFields = get(DOCUMENT_FIELDS, values, Collections.emptyMap());

        SearchHit searchHit = new SearchHit(-1, id, nestedIdentity);
        searchHit.addDocumentFields(documentFields, metaFields);
        String index = get(Fields._INDEX, values, null);
        String clusterAlias = null;
        if (index != null) {
            int indexOf = index.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
            if (indexOf > 0) {
                clusterAlias = index.substring(0, indexOf);
                index = index.substring(indexOf + 1);
            }
        }
        ShardId shardId = get(Fields._SHARD, values, null);
        String nodeId = get(Fields._NODE, values, null);
        if (shardId != null && nodeId != null) {
            assert shardId.getIndexName().equals(index);
            searchHit.shard(new SearchShardTarget(nodeId, shardId, clusterAlias));
        } else {
            // these fields get set anyways when setting the shard target,
            // but we set them explicitly when we don't have enough info to rebuild the shard target
            searchHit.index = index;
            searchHit.clusterAlias = clusterAlias;
        }
        searchHit.score(get(Fields._SCORE, values, DEFAULT_SCORE));
        searchHit.setRank(get(Fields._RANK, values, NO_RANK));
        searchHit.version(get(Fields._VERSION, values, -1L));
        searchHit.setSeqNo(get(Fields._SEQ_NO, values, SequenceNumbers.UNASSIGNED_SEQ_NO));
        searchHit.setPrimaryTerm(get(Fields._PRIMARY_TERM, values, SequenceNumbers.UNASSIGNED_PRIMARY_TERM));
        searchHit.sortValues(get(Fields.SORT, values, SearchSortValues.EMPTY));
        searchHit.highlightFields(get(Fields.HIGHLIGHT, values, null));
        searchHit.sourceRef(get(SourceFieldMapper.NAME, values, null));
        searchHit.explanation(get(Fields._EXPLANATION, values, null));
        searchHit.setInnerHits(get(Fields.INNER_HITS, values, null));
        searchHit.matchedQueries(get(Fields.MATCHED_QUERIES, values, null));
        return searchHit;
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(String key, Map<String, Object> map, T defaultValue) {
        return (T) map.getOrDefault(key, defaultValue);
    }

    private static float parseScore(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER || parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return parser.floatValue();
        } else {
            return Float.NaN;
        }
    }

    private static BytesReference parseSourceBytes(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
            // the original document gets slightly modified: whitespaces or
            // pretty printing are not preserved,
            // it all depends on the current builder settings
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    private static Map<String, DocumentField> parseFields(XContentParser parser) throws IOException {
        Map<String, DocumentField> fields = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            DocumentField field = DocumentField.fromXContent(parser);
            fields.put(field.getName(), field);
        }
        return fields;
    }

    private static Map<String, SearchHits> parseInnerHits(XContentParser parser) throws IOException {
        Map<String, SearchHits> innerHits = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String name = parser.currentName();
            ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser);
            ensureFieldName(parser, parser.nextToken(), SearchHits.Fields.HITS);
            innerHits.put(name, SearchHits.fromXContent(parser));
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        }
        return innerHits;
    }

    private static Map<String, HighlightField> parseHighlightFields(XContentParser parser) throws IOException {
        Map<String, HighlightField> highlightFields = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            HighlightField highlightField = HighlightField.fromXContent(parser);
            highlightFields.put(highlightField.getName(), highlightField);
        }
        return highlightFields;
    }

    private static Explanation parseExplanation(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        XContentParser.Token token;
        Float value = null;
        String description = null;
        List<Explanation> details = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (Fields.VALUE.equals(currentFieldName)) {
                value = parser.floatValue();
            } else if (Fields.DESCRIPTION.equals(currentFieldName)) {
                description = parser.textOrNull();
            } else if (Fields.DETAILS.equals(currentFieldName)) {
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    details.add(parseExplanation(parser));
                }
            } else {
                parser.skipChildren();
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

    private static void buildExplanation(XContentBuilder builder, Explanation explanation) throws IOException {
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
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SearchHit other = (SearchHit) obj;
        return Objects.equals(id, other.id)
            && Objects.equals(nestedIdentity, other.nestedIdentity)
            && Objects.equals(version, other.version)
            && Objects.equals(seqNo, other.seqNo)
            && Objects.equals(primaryTerm, other.primaryTerm)
            && Objects.equals(source, other.source)
            && Objects.equals(documentFields, other.documentFields)
            && Objects.equals(metaFields, other.metaFields)
            && Objects.equals(getHighlightFields(), other.getHighlightFields())
            && Objects.equals(getMatchedQueriesAndScores(), other.getMatchedQueriesAndScores())
            && Objects.equals(explanation, other.explanation)
            && Objects.equals(shard, other.shard)
            && Objects.equals(innerHits, other.innerHits)
            && Objects.equals(index, other.index)
            && Objects.equals(clusterAlias, other.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            nestedIdentity,
            version,
            seqNo,
            primaryTerm,
            source,
            documentFields,
            metaFields,
            getHighlightFields(),
            getMatchedQueriesAndScores(),
            explanation,
            shard,
            innerHits,
            index,
            clusterAlias
        );
    }

    /**
     * Encapsulates the nested identity of a hit.
     */
    public static final class NestedIdentity implements Writeable, ToXContentFragment {

        private static final String _NESTED = "_nested";
        private static final String FIELD = "field";
        private static final String OFFSET = "offset";

        private final Text field;
        private final int offset;
        private final NestedIdentity child;

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

        /**
         * Extracts the part of the root source that applies to this particular NestedIdentity, while
         * preserving the enclosing path structure.
         *
         * For a root document that looks like this:
         * { "children" :
         *    [
         *      { "grandchildren" : [ { "field" : "value1" }, { "field" : "value2" } ] },
         *      { "grandchildren" : [ { "field" : "value3" }, { "field" : "value4" } ] }
         *   ]
         * }
         *
         * Extracting the NestedIdentity of the first child and second grandchild results in a source that looks like this:
         * { "children" : { "grandchildren" : { "field" : "value2" } } }
         *
         * If the relevant child source object does not exist in the root, then we return {@link Source#empty(XContentType)}
         */
        @SuppressWarnings("unchecked")
        public Source extractSource(Source root) {
            // Isolate the nested json array object that matches with nested hit and wrap it back into the same json
            // structure with the nested json array object being the actual content. The latter is important, so that
            // features like source filtering and highlighting work consistent regardless of whether the field points
            // to a json object array for consistency reasons on how we refer to fields
            Map<String, Object> rootSourceAsMap = root.source();
            Map<String, Object> nestedSourceAsMap = new HashMap<>();
            Map<String, Object> current = nestedSourceAsMap;
            for (SearchHit.NestedIdentity nested = this; nested != null; nested = nested.getChild()) {
                String nestedPath = nested.getField().string();
                current.put(nestedPath, new HashMap<>());
                List<Map<?, ?>> nestedParsedSource = XContentMapValues.extractNestedSources(nestedPath, rootSourceAsMap);
                if (nestedParsedSource == null) {
                    return Source.empty(root.sourceContentType());
                }
                if (nested.getOffset() > nestedParsedSource.size() - 1) {
                    throw new IllegalStateException("Error retrieving path " + this.field);
                }
                rootSourceAsMap = (Map<String, Object>) nestedParsedSource.get(nested.getOffset());
                if (nested.getChild() == null) {
                    current.put(nestedPath, rootSourceAsMap);
                } else {
                    Map<String, Object> next = new HashMap<>();
                    current.put(nestedPath, next);
                    current = next;
                }
            }
            return Source.fromMap(nestedSourceAsMap, root.sourceContentType());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalText(field);
            out.writeInt(offset);
            out.writeOptionalWriteable(child);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(_NESTED);
            return innerToXContent(builder, params);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        /**
         * Rendering of the inner XContent object without the leading field name. This way the structure innerToXContent renders and
         * fromXContent parses correspond to each other.
         */
        XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (field != null) {
                builder.field(FIELD, field);
            }
            if (offset != -1) {
                builder.field(OFFSET, offset);
            }
            if (child != null) {
                builder = child.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<NestedIdentity, Void> PARSER = new ConstructingObjectParser<>(
            "nested_identity",
            true,
            ctorArgs -> new NestedIdentity((String) ctorArgs[0], (int) ctorArgs[1], (NestedIdentity) ctorArgs[2])
        );
        static {
            PARSER.declareString(constructorArg(), new ParseField(FIELD));
            PARSER.declareInt(constructorArg(), new ParseField(OFFSET));
            PARSER.declareObject(optionalConstructorArg(), PARSER, new ParseField(_NESTED));
        }

        static NestedIdentity fromXContent(XContentParser parser, Void context) {
            return fromXContent(parser);
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
            return Objects.equals(field, other.field) && Objects.equals(offset, other.offset) && Objects.equals(child, other.child);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, offset, child);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
