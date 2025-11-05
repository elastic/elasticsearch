/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.fetch.subphase.LookupField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.lucene.Lucene.readExplanation;
import static org.elasticsearch.common.lucene.Lucene.writeExplanation;

/**
 * A single search hit.
 *
 * @see SearchHits
 */
public final class SearchHit implements Writeable, ToXContentObject, RefCounted {

    private static final TransportVersion DOC_FIELDS_AS_LIST = TransportVersion.fromName("doc_fields_as_list");

    private final transient int docId;

    static final float DEFAULT_SCORE = Float.NaN;
    private float score;

    static final int NO_RANK = -1;
    private int rank;

    private final Text id;

    private final NestedIdentity nestedIdentity;

    private long version;
    private long seqNo;
    private long primaryTerm;

    private BytesReference source;

    private final Map<String, DocumentField> documentFields;
    private final Map<String, DocumentField> metaFields;

    private Map<String, HighlightField> highlightFields;

    private SearchSortValues sortValues;

    private Map<String, Float> matchedQueries;

    private Explanation explanation;

    @Nullable
    private SearchShardTarget shard;

    // These two fields normally get set when setting the shard target, so they hold the same values as the target thus don't get
    // serialized over the wire. When parsing hits back from xcontent though, in most of the cases (whenever explanation is disabled)
    // we can't rebuild the shard target object so we need to set these manually for users retrieval.
    private transient String index;
    private transient String clusterAlias;

    // For asserting that the method #getSourceAsMap is called just once on the lifetime of this object
    private boolean sourceAsMapCalled = false;

    private Map<String, SearchHits> innerHits;

    private final RefCounted refCounted;

    /**
     * Constructs a new SearchHit with the specified document ID.
     *
     * @param docId the Lucene document ID
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * }</pre>
     */
    public SearchHit(int docId) {
        this(docId, null);
    }

    /**
     * Constructs a new SearchHit with the specified document ID and document identifier.
     *
     * @param docId the Lucene document ID
     * @param id the document identifier (may be null)
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123, "doc-id-456");
     * }</pre>
     */
    public SearchHit(int docId, String id) {
        this(docId, id, null);
    }

    /**
     * Constructs a new SearchHit with the specified document ID, identifier, and nested identity.
     *
     * @param nestedTopDocId the Lucene document ID of the nested top document
     * @param id the document identifier (may be null)
     * @param nestedIdentity the nested document identity information (may be null)
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NestedIdentity identity = new NestedIdentity("nested_field", 0, null);
     * SearchHit hit = new SearchHit(123, "doc-id-456", identity);
     * }</pre>
     */
    public SearchHit(int nestedTopDocId, String id, NestedIdentity nestedIdentity) {
        this(nestedTopDocId, id, nestedIdentity, null);
    }

    private SearchHit(int nestedTopDocId, String id, NestedIdentity nestedIdentity, @Nullable RefCounted refCounted) {
        this(
            nestedTopDocId,
            DEFAULT_SCORE,
            NO_RANK,
            id == null ? null : new Text(id),
            nestedIdentity,
            -1,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            null,
            null,
            SearchSortValues.EMPTY,
            Collections.emptyMap(),
            null,
            null,
            null,
            null,
            null,
            new HashMap<>(),
            new HashMap<>(),
            refCounted
        );
    }

    public SearchHit(
        int docId,
        float score,
        int rank,
        Text id,
        NestedIdentity nestedIdentity,
        long version,
        long seqNo,
        long primaryTerm,
        BytesReference source,
        Map<String, HighlightField> highlightFields,
        SearchSortValues sortValues,
        Map<String, Float> matchedQueries,
        Explanation explanation,
        SearchShardTarget shard,
        String index,
        String clusterAlias,
        Map<String, SearchHits> innerHits,
        Map<String, DocumentField> documentFields,
        Map<String, DocumentField> metaFields,
        @Nullable RefCounted refCounted
    ) {
        this.docId = docId;
        this.score = score;
        this.rank = rank;
        this.id = id;
        this.nestedIdentity = nestedIdentity;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.source = source;
        this.highlightFields = highlightFields;
        this.sortValues = sortValues;
        this.matchedQueries = matchedQueries;
        this.explanation = explanation;
        this.shard = shard;
        this.index = index;
        this.clusterAlias = clusterAlias;
        this.innerHits = innerHits;
        this.documentFields = documentFields;
        this.metaFields = metaFields;
        this.refCounted = refCounted == null ? LeakTracker.wrap(new SimpleRefCounted()) : refCounted;
    }

    /**
     * Reads a SearchHit from the provided stream input.
     *
     * @param in the stream input to read from
     * @param pooled whether to use pooled (ref-counted) instances for memory efficiency
     * @return the SearchHit instance read from the stream
     * @throws IOException if an I/O error occurs during deserialization
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * StreamInput in = ...;
     * SearchHit hit = SearchHit.readFrom(in, true);
     * }</pre>
     */
    public static SearchHit readFrom(StreamInput in, boolean pooled) throws IOException {
        final float score = in.readFloat();
        final int rank;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            rank = in.readVInt();
        } else {
            rank = NO_RANK;
        }
        final Text id = in.readOptionalText();
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            in.readOptionalText();
        }
        final NestedIdentity nestedIdentity = in.readOptionalWriteable(NestedIdentity::new);
        final long version = in.readLong();
        final long seqNo = in.readZLong();
        final long primaryTerm = in.readVLong();
        BytesReference source = pooled ? in.readReleasableBytesReference() : in.readBytesReference();
        if (source.length() == 0) {
            source = null;
        }
        Explanation explanation = null;
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        final Map<String, DocumentField> documentFields;
        final Map<String, DocumentField> metaFields;
        if (in.getTransportVersion().supports(DOC_FIELDS_AS_LIST)) {
            documentFields = DocumentField.readFieldsFromMapValues(in);
            metaFields = DocumentField.readFieldsFromMapValues(in);
        } else {
            documentFields = in.readMap(DocumentField::new);
            metaFields = in.readMap(DocumentField::new);
        }
        Map<String, HighlightField> highlightFields = in.readMapValues(HighlightField::new, HighlightField::name);
        highlightFields = highlightFields.isEmpty() ? null : unmodifiableMap(highlightFields);

        final SearchSortValues sortValues = SearchSortValues.readFrom(in);

        final Map<String, Float> matchedQueries;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            matchedQueries = in.readOrderedMap(StreamInput::readString, StreamInput::readFloat);
        } else {
            int size = in.readVInt();
            matchedQueries = Maps.newLinkedHashMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                matchedQueries.put(in.readString(), Float.NaN);
            }
        }

        final SearchShardTarget shardTarget = in.readOptionalWriteable(SearchShardTarget::new);
        final String index;
        final String clusterAlias;
        if (shardTarget == null) {
            index = null;
            clusterAlias = null;
        } else {
            index = shardTarget.getIndex();
            clusterAlias = shardTarget.getClusterAlias();
        }

        boolean isPooled = pooled && source != null;
        final Map<String, SearchHits> innerHits;
        int size = in.readVInt();
        if (size > 0) {
            innerHits = Maps.newMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                var key = in.readString();
                var nestedHits = SearchHits.readFrom(in, pooled);
                innerHits.put(key, nestedHits);
                isPooled = isPooled || nestedHits.isPooled();
            }
        } else {
            innerHits = null;
        }
        return new SearchHit(
            -1,
            score,
            rank,
            id,
            nestedIdentity,
            version,
            seqNo,
            primaryTerm,
            source,
            highlightFields,
            sortValues,
            matchedQueries,
            explanation,
            shardTarget,
            index,
            clusterAlias,
            innerHits,
            documentFields,
            metaFields,
            isPooled ? null : ALWAYS_REFERENCED
        );
    }

    /**
     * Creates an unpooled SearchHit that doesn't require manual reference counting.
     * Unpooled hits are not automatically deallocated and are suitable for long-lived objects.
     *
     * @param docId the Lucene document ID
     * @return an unpooled SearchHit instance
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = SearchHit.unpooled(123);
     * // No need to call decRef() on unpooled hits
     * }</pre>
     */
    public static SearchHit unpooled(int docId) {
        return unpooled(docId, null);
    }

    /**
     * Creates an unpooled SearchHit with the specified document ID and identifier.
     * Unpooled hits are not automatically deallocated and are suitable for long-lived objects.
     *
     * @param docId the Lucene document ID
     * @param id the document identifier (may be null)
     * @return an unpooled SearchHit instance
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = SearchHit.unpooled(123, "doc-id-456");
     * // No need to call decRef() on unpooled hits
     * }</pre>
     */
    public static SearchHit unpooled(int docId, String id) {
        return unpooled(docId, id, null);
    }

    /**
     * Creates an unpooled SearchHit with full specification of document identity.
     * Unpooled hits are not automatically deallocated and are suitable for long-lived objects.
     *
     * @param nestedTopDocId the Lucene document ID of the nested top document
     * @param id the document identifier (may be null)
     * @param nestedIdentity the nested document identity information (may be null)
     * @return an unpooled SearchHit instance
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NestedIdentity identity = new NestedIdentity("nested_field", 0, null);
     * SearchHit hit = SearchHit.unpooled(123, "doc-id-456", identity);
     * // No need to call decRef() on unpooled hits
     * }</pre>
     */
    public static SearchHit unpooled(int nestedTopDocId, String id, NestedIdentity nestedIdentity) {
        // always referenced search hits do NOT call #deallocate
        return new SearchHit(nestedTopDocId, id, nestedIdentity, ALWAYS_REFERENCED);
    }

    private static final Text SINGLE_MAPPING_TYPE = new Text(MapperService.SINGLE_MAPPING_NAME);

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert hasReferences();
        out.writeFloat(score);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeVInt(rank);
        } else if (rank != NO_RANK) {
            throw new IllegalArgumentException("cannot serialize [rank] to version [" + out.getTransportVersion().toReleaseVersion() + "]");
        }
        out.writeOptionalText(id);
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
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
        if (out.getTransportVersion().supports(DOC_FIELDS_AS_LIST)) {
            out.writeMapValues(documentFields);
            out.writeMapValues(metaFields);
        } else {
            out.writeMap(documentFields, StreamOutput::writeWriteable);
            out.writeMap(metaFields, StreamOutput::writeWriteable);
        }
        if (highlightFields == null) {
            out.writeVInt(0);
        } else {
            out.writeCollection(highlightFields.values());
        }
        sortValues.writeTo(out);

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeMap(matchedQueries, StreamOutput::writeFloat);
        } else {
            out.writeStringCollection(matchedQueries.keySet());
        }
        out.writeOptionalWriteable(shard);
        if (innerHits == null) {
            out.writeVInt(0);
        } else {
            out.writeMap(innerHits, StreamOutput::writeWriteable);
        }
    }

    /**
     * Returns the Lucene document ID for this search hit.
     *
     * @return the document ID
     */
    public int docId() {
        return this.docId;
    }

    /**
     * Sets the score for this search hit.
     *
     * @param score the relevance score to set
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * hit.score(0.85f);
     * }</pre>
     */
    public void score(float score) {
        this.score = score;
    }

    /**
     * Returns the relevance score of this search hit.
     * Returns {@link Float#NaN} if scoring was disabled for the query.
     *
     * @return the score, or {@link Float#NaN} if not scored
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * float score = hit.getScore();
     * if (!Float.isNaN(score)) {
     *     // Process the score
     * }
     * }</pre>
     */
    public float getScore() {
        return this.score;
    }

    /**
     * Sets the rank position of this hit in the search results.
     *
     * @param rank the rank position (0-based)
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * hit.setRank(5); // 6th result
     * }</pre>
     */
    public void setRank(int rank) {
        this.rank = rank;
    }

    /**
     * Returns the rank position of this hit in the search results.
     * Returns {@link #NO_RANK} (-1) if ranking was not applied.
     *
     * @return the rank position, or {@link #NO_RANK} if not ranked
     */
    public int getRank() {
        return this.rank;
    }

    /**
     * Sets the document version for this search hit.
     *
     * @param version the document version
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * hit.version(3L); // Document is at version 3
     * }</pre>
     */
    public void version(long version) {
        this.version = version;
    }

    /**
     * Returns the document version of this hit.
     * Returns -1 if version was not requested or not available.
     *
     * @return the version, or -1 if not available
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Sets the sequence number for this document.
     * The sequence number is used for optimistic concurrency control.
     *
     * @param seqNo the sequence number
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * hit.setSeqNo(42L);
     * }</pre>
     */
    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    /**
     * Sets the primary term for this document.
     * The primary term is used for optimistic concurrency control.
     *
     * @param primaryTerm the primary term
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * hit.setPrimaryTerm(1L);
     * }</pre>
     */
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
     * Returns the name of the index this hit belongs to.
     *
     * @return the index name, or null if not set
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * String indexName = hit.getIndex();
     * System.out.println("Hit from index: " + indexName);
     * }</pre>
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * Returns the unique identifier of the document.
     *
     * @return the document ID, or null if not set
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * String docId = hit.getId();
     * System.out.println("Document ID: " + docId);
     * }</pre>
     */
    public String getId() {
        return id != null ? id.string() : null;
    }

    /**
     * Returns the nested document identity information if this is a nested hit.
     *
     * @return the nested identity, or null if this is not a nested document
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * NestedIdentity nested = hit.getNestedIdentity();
     * if (nested != null) {
     *     String field = nested.getField().string();
     *     int offset = nested.getOffset();
     * }
     * }</pre>
     */
    public NestedIdentity getNestedIdentity() {
        return nestedIdentity;
    }

    /**
     * Returns the source document as a bytes reference, decompressing if necessary.
     * The source is the original JSON document that was indexed.
     *
     * @return the source bytes reference, or null if source is not available
     * @throws ElasticsearchParseException if decompression fails
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * BytesReference sourceRef = hit.getSourceRef();
     * if (sourceRef != null) {
     *     // Process source bytes
     * }
     * }</pre>
     */
    public BytesReference getSourceRef() {
        assert hasReferences();
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
     * Sets the source document for this hit. The source may be compressed.
     *
     * @param source the source bytes reference (may be compressed)
     * @return this SearchHit instance for method chaining
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * BytesReference source = ...;
     * hit.sourceRef(source);
     * }</pre>
     */
    public SearchHit sourceRef(BytesReference source) {
        this.source = source;
        return this;
    }

    /**
     * Checks whether the source is available for this hit.
     * Returns false if the _source field was not requested or if source is disabled in the mapping.
     *
     * @return true if source is available, false otherwise
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * if (hit.hasSource()) {
     *     Map<String, Object> source = hit.getSourceAsMap();
     * }
     * }</pre>
     */
    public boolean hasSource() {
        assert hasReferences();
        return source != null;
    }

    /**
     * Returns the source document as a JSON string.
     *
     * @return the source as a JSON string, or null if source is not available
     * @throws ElasticsearchParseException if conversion to JSON fails
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * String jsonSource = hit.getSourceAsString();
     * System.out.println("Source: " + jsonSource);
     * }</pre>
     */
    public String getSourceAsString() {
        assert hasReferences();
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
     * Returns the source document as a Map.
     * <p>
     * <strong>Important:</strong> This method is expensive and should be called at most once
     * during the lifetime of the object, as the generated map is not cached.
     *
     * @return the source as a Map, or null if source is not available
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Map<String, Object> source = hit.getSourceAsMap();
     * if (source != null) {
     *     Object field = source.get("field_name");
     * }
     * }</pre>
     */
    public Map<String, Object> getSourceAsMap() {
        assert hasReferences();
        assert sourceAsMapCalled == false : "getSourceAsMap() called twice";
        sourceAsMapCalled = true;
        if (source == null) {
            return null;
        }
        return Source.fromBytes(source).source();
    }

    /**
     * Returns the DocumentField matching the given field name.
     * This includes both document fields and metadata fields.
     *
     * @param fieldName the name of the field to retrieve
     * @return the DocumentField, or null if the field is not present
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * DocumentField field = hit.field("my_field");
     * if (field != null) {
     *     List<Object> values = field.getValues();
     * }
     * }</pre>
     */
    public DocumentField field(String fieldName) {
        assert hasReferences();
        DocumentField result = documentFields.get(fieldName);
        if (result != null) {
            return result;
        } else {
            return metaFields.get(fieldName);
        }
    }

    /**
     * Adds a DocumentField to this hit.
     * Does nothing if the field parameter is null.
     *
     * @param field the DocumentField to add
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * DocumentField field = new DocumentField("my_field", List.of("value1", "value2"));
     * hit.setDocumentField(field);
     * }</pre>
     */
    public void setDocumentField(DocumentField field) {
        if (field == null) return;
        this.documentFields.put(field.getName(), field);
    }

    /**
     * Adds multiple document fields and metadata fields to this hit.
     *
     * @param docFields the document fields to add
     * @param metaFields the metadata fields to add
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * Map<String, DocumentField> docFields = new HashMap<>();
     * Map<String, DocumentField> metaFields = new HashMap<>();
     * hit.addDocumentFields(docFields, metaFields);
     * }</pre>
     */
    public void addDocumentFields(Map<String, DocumentField> docFields, Map<String, DocumentField> metaFields) {
        this.documentFields.putAll(docFields);
        this.metaFields.putAll(metaFields);
    }

    /**
     * Removes a document field from this hit.
     *
     * @param field the name of the field to remove
     * @return the removed DocumentField, or null if the field was not present
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * DocumentField removed = hit.removeDocumentField("my_field");
     * }</pre>
     */
    public DocumentField removeDocumentField(String field) {
        return documentFields.remove(field);
    }

    /**
     * Returns an unmodifiable map of metadata fields for this hit.
     * Metadata fields include system fields like _index, _id, _version, etc.
     *
     * @return an unmodifiable map of metadata fields
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Map<String, DocumentField> metaFields = hit.getMetadataFields();
     * DocumentField routing = metaFields.get("_routing");
     * }</pre>
     */
    public Map<String, DocumentField> getMetadataFields() {
        assert hasReferences();
        return Collections.unmodifiableMap(metaFields);
    }

    /**
     * Returns an unmodifiable map of non-metadata (document) fields requested for this hit.
     * These are the fields explicitly requested in the search request.
     *
     * @return an unmodifiable map of document fields
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Map<String, DocumentField> docFields = hit.getDocumentFields();
     * DocumentField myField = docFields.get("my_field");
     * }</pre>
     */
    public Map<String, DocumentField> getDocumentFields() {
        assert hasReferences();
        return Collections.unmodifiableMap(documentFields);
    }

    /**
     * Returns a map of all hit fields (document and metadata fields combined).
     * This is the union of document fields and metadata fields.
     *
     * @return a map containing both document and metadata fields, or an empty map if none exist
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Map<String, DocumentField> allFields = hit.getFields();
     * for (Map.Entry<String, DocumentField> entry : allFields.entrySet()) {
     *     System.out.println(entry.getKey() + ": " + entry.getValue().getValues());
     * }
     * }</pre>
     */
    public Map<String, DocumentField> getFields() {
        assert hasReferences();
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
     * Checks whether this search hit contains any lookup fields.
     * Lookup fields are fields that reference values from other documents.
     *
     * @return true if any document fields contain lookup field references, false otherwise
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * if (hit.hasLookupFields()) {
     *     // Resolve lookup fields
     *     Map<LookupField, List<Object>> results = ...;
     *     hit.resolveLookupFields(results);
     * }
     * }</pre>
     */
    public boolean hasLookupFields() {
        return getDocumentFields().values().stream().anyMatch(doc -> doc.getLookupFields().isEmpty() == false);
    }

    /**
     * Resolves lookup fields with the given results and merges them as regular fetch fields.
     * Lookup fields are replaced with their resolved values.
     *
     * @param lookupResults a map of lookup fields to their resolved values
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Map<LookupField, List<Object>> lookupResults = new HashMap<>();
     * // ... populate lookupResults
     * hit.resolveLookupFields(lookupResults);
     * }</pre>
     */
    public void resolveLookupFields(Map<LookupField, List<Object>> lookupResults) {
        assert hasReferences();
        if (lookupResults.isEmpty()) {
            return;
        }
        for (Iterator<Map.Entry<String, DocumentField>> iterator = documentFields.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, DocumentField> entry = iterator.next();
            final DocumentField docField = entry.getValue();
            if (docField.getLookupFields().isEmpty()) {
                continue;
            }
            final List<Object> newValues = new ArrayList<>(docField.getValues());
            for (LookupField lookupField : docField.getLookupFields()) {
                final List<Object> resolvedValues = lookupResults.get(lookupField);
                if (resolvedValues != null) {
                    newValues.addAll(resolvedValues);
                }
            }
            if (newValues.isEmpty() && docField.getIgnoredValues().isEmpty()) {
                iterator.remove();
            } else {
                entry.setValue(new DocumentField(docField.getName(), newValues, docField.getIgnoredValues()));
            }
        }
        assert hasLookupFields() == false : "Some lookup fields are not resolved";
    }

    /**
     * Returns a map of highlighted fields for this hit.
     * Highlights show matching query terms within field values.
     *
     * @return a map of highlighted fields, or an empty map if no highlights are available
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Map<String, HighlightField> highlights = hit.getHighlightFields();
     * HighlightField titleHighlight = highlights.get("title");
     * if (titleHighlight != null) {
     *     for (Text fragment : titleHighlight.fragments()) {
     *         System.out.println(fragment.string());
     *     }
     * }
     * }</pre>
     */
    public Map<String, HighlightField> getHighlightFields() {
        assert hasReferences();
        return highlightFields == null ? emptyMap() : highlightFields;
    }

    /**
     * Sets the highlighted fields for this hit.
     *
     * @param highlightFields the map of highlighted fields to set
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * Map<String, HighlightField> highlights = new HashMap<>();
     * hit.highlightFields(highlights);
     * }</pre>
     */
    public void highlightFields(Map<String, HighlightField> highlightFields) {
        this.highlightFields = highlightFields;
    }

    /**
     * Sets the sort values for this hit using raw values and formats.
     *
     * @param sortValues the raw sort values
     * @param sortValueFormats the formats for the sort values
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * Object[] values = new Object[]{100, "text"};
     * DocValueFormat[] formats = new DocValueFormat[]{DocValueFormat.RAW, DocValueFormat.RAW};
     * hit.sortValues(values, formats);
     * }</pre>
     */
    public void sortValues(Object[] sortValues, DocValueFormat[] sortValueFormats) {
        sortValues(new SearchSortValues(sortValues, sortValueFormats));
    }

    /**
     * Sets the sort values for this hit.
     *
     * @param sortValues the SearchSortValues instance containing sort information
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * SearchSortValues sortValues = ...;
     * hit.sortValues(sortValues);
     * }</pre>
     */
    public void sortValues(SearchSortValues sortValues) {
        this.sortValues = sortValues;
    }

    /**
     * Returns an array of the formatted sort values used to sort this hit.
     * These are the human-readable versions of the sort values.
     *
     * @return an array of formatted sort values
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Object[] sortValues = hit.getSortValues();
     * for (Object value : sortValues) {
     *     System.out.println("Sort value: " + value);
     * }
     * }</pre>
     */
    public Object[] getSortValues() {
        return sortValues.getFormattedSortValues();
    }

    /**
     * Returns an array of the raw (unformatted) sort values used to sort this hit.
     * These are the internal representation of sort values.
     *
     * @return an array of raw sort values
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Object[] rawSortValues = hit.getRawSortValues();
     * }</pre>
     */
    public Object[] getRawSortValues() {
        return sortValues.getRawSortValues();
    }

    /**
     * Returns the explanation of why this document matched the query, if requested.
     * The explanation provides detailed information about the scoring process.
     *
     * @return the Explanation object, or null if explanation was not requested
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Explanation explanation = hit.getExplanation();
     * if (explanation != null) {
     *     System.out.println("Score explanation: " + explanation.getDescription());
     *     System.out.println("Score value: " + explanation.getValue());
     * }
     * }</pre>
     */
    public Explanation getExplanation() {
        return explanation;
    }

    /**
     * Sets the explanation for this hit.
     *
     * @param explanation the Lucene Explanation object
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * Explanation explanation = ...;
     * hit.explanation(explanation);
     * }</pre>
     */
    public void explanation(Explanation explanation) {
        this.explanation = explanation;
    }

    /**
     * Returns the shard target information for this search hit.
     * This indicates which shard and node the hit came from.
     *
     * @return the SearchShardTarget, or null if not set
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * SearchShardTarget shard = hit.getShard();
     * if (shard != null) {
     *     System.out.println("Shard: " + shard.getShardId());
     *     System.out.println("Node: " + shard.getNodeId());
     * }
     * }</pre>
     */
    public SearchShardTarget getShard() {
        return shard;
    }

    /**
     * Sets the shard target information for this hit and all its inner hits.
     *
     * @param target the SearchShardTarget to set
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * SearchShardTarget target = new SearchShardTarget("node1", shardId, null);
     * hit.shard(target);
     * }</pre>
     */
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
     * Returns the cluster alias this hit comes from, or null if it comes from a local cluster.
     * Used in cross-cluster search scenarios.
     *
     * @return the cluster alias, or null if from local cluster
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * String cluster = hit.getClusterAlias();
     * if (cluster != null) {
     *     System.out.println("Hit from remote cluster: " + cluster);
     * }
     * }</pre>
     */
    public String getClusterAlias() {
        return clusterAlias;
    }

    /**
     * Sets the map of matched named queries and their scores.
     *
     * @param matchedQueries the map of query names to scores
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * Map<String, Float> queries = new HashMap<>();
     * queries.put("my_query", 0.9f);
     * hit.matchedQueries(queries);
     * }</pre>
     */
    public void matchedQueries(Map<String, Float> matchedQueries) {
        this.matchedQueries = matchedQueries;
    }

    /**
     * Returns the names of all named queries that matched this document.
     * Named queries are useful for tracking which parts of a compound query matched.
     *
     * @return an array of matched query names, or an empty array if none matched
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * String[] matchedQueries = hit.getMatchedQueries();
     * for (String queryName : matchedQueries) {
     *     System.out.println("Matched query: " + queryName);
     * }
     * }</pre>
     */
    public String[] getMatchedQueries() {
        return matchedQueries == null ? new String[0] : matchedQueries.keySet().toArray(new String[0]);
    }

    /**
     * Returns the score of a specific named query if it matched this document.
     *
     * @param name the name of the query
     * @return the score of the named query, or null if the query didn't match
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Float score = hit.getMatchedQueryScore("my_query");
     * if (score != null) {
     *     System.out.println("Query score: " + score);
     * }
     * }</pre>
     */
    public Float getMatchedQueryScore(String name) {
        return getMatchedQueriesAndScores().get(name);
    }

    /**
     * Returns a map of all matched named queries and their associated scores.
     *
     * @return a map of query names to scores, or an empty map if no queries matched
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Map<String, Float> matchedQueries = hit.getMatchedQueriesAndScores();
     * matchedQueries.forEach((name, score) -> {
     *     System.out.println(name + ": " + score);
     * });
     * }</pre>
     */
    public Map<String, Float> getMatchedQueriesAndScores() {
        return matchedQueries == null ? Collections.emptyMap() : matchedQueries;
    }

    /**
     * Returns the inner hits (nested or parent-child) associated with this hit.
     * Inner hits allow retrieving nested documents or related documents.
     *
     * @return a map of inner hit names to SearchHits, or null if there are no inner hits
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = ...;
     * Map<String, SearchHits> innerHits = hit.getInnerHits();
     * if (innerHits != null) {
     *     SearchHits nestedHits = innerHits.get("nested_field");
     *     for (SearchHit nestedHit : nestedHits) {
     *         // Process nested hit
     *     }
     * }
     * }</pre>
     */
    public Map<String, SearchHits> getInnerHits() {
        assert hasReferences();
        return innerHits;
    }

    /**
     * Sets the inner hits for this search hit.
     * This method can only be called once per SearchHit instance.
     *
     * @param innerHits the map of inner hit names to SearchHits
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchHit hit = new SearchHit(123);
     * Map<String, SearchHits> innerHits = new HashMap<>();
     * hit.setInnerHits(innerHits);
     * }</pre>
     */
    public void setInnerHits(Map<String, SearchHits> innerHits) {
        assert innerHits == null || innerHits.values().stream().noneMatch(h -> h.hasReferences() == false);
        assert this.innerHits == null;
        this.innerHits = innerHits;
    }

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        if (refCounted.decRef()) {
            deallocate();
            return true;
        }
        return false;
    }

    private void deallocate() {
        if (SearchHit.this.innerHits != null) {
            for (SearchHits h : SearchHit.this.innerHits.values()) {
                h.decRef();
            }
            SearchHit.this.innerHits = null;
        }
        if (SearchHit.this.source instanceof RefCounted r) {
            r.decRef();
        }
        SearchHit.this.source = null;
        clearIfMutable(documentFields);
        clearIfMutable(metaFields);
        this.highlightFields = null;
    }

    private static void clearIfMutable(Map<String, DocumentField> fields) {
        // check that we're dealing with a HashMap, instances read from the wire that are empty be of an immutable type
        assert fields instanceof HashMap<?, ?> || fields.isEmpty() : fields;
        if (fields instanceof HashMap<?, ?> hm) {
            hm.clear();
        }
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
    }

    public SearchHit asUnpooled() {
        assert hasReferences();
        if (isPooled() == false) {
            return this;
        }
        return new SearchHit(
            docId,
            score,
            rank,
            id,
            nestedIdentity,
            version,
            seqNo,
            primaryTerm,
            source instanceof RefCounted ? new BytesArray(source.toBytesRef(), true) : source,
            highlightFields,
            sortValues,
            matchedQueries,
            explanation,
            shard,
            index,
            clusterAlias,
            innerHits == null
                ? null
                : innerHits.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().asUnpooled())),
            cloneIfHashMap(documentFields),
            cloneIfHashMap(metaFields),
            ALWAYS_REFERENCED
        );
    }

    private Map<String, DocumentField> cloneIfHashMap(Map<String, DocumentField> map) {
        return map instanceof HashMap<String, DocumentField> hashMap ? new HashMap<>(hashMap) : map;
    }

    public boolean isPooled() {
        return refCounted != ALWAYS_REFERENCED;
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
        assert hasReferences();
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
            if (IgnoredFieldMapper.NAME.equals(field.getName()) || field.getName().equals(IgnoredSourceFieldMapper.NAME)) {
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

        static final String _NESTED = "_nested";
        static final String FIELD = "field";
        static final String OFFSET = "offset";

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
