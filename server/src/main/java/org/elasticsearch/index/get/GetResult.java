/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.get;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.rest.action.document.RestMultiGetAction;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class GetResult implements Writeable, Iterable<DocumentField>, ToXContentObject {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(GetResult.class);

    public static final String _INDEX = "_index";
    public static final String _ID = "_id";
    private static final String _VERSION = "_version";
    private static final String _SEQ_NO = "_seq_no";
    private static final String _PRIMARY_TERM = "_primary_term";
    private static final String FOUND = "found";
    private static final String FIELDS = "fields";

    private String index;
    private String id;
    private long version;
    private long seqNo;
    private long primaryTerm;
    private boolean exists;
    private final Map<String, DocumentField> documentFields;
    private final Map<String, DocumentField> metaFields;
    private Map<String, Object> sourceAsMap;
    private BytesReference source;
    private byte[] sourceAsBytes;

    public GetResult(StreamInput in) throws IOException {
        index = in.readString();
        if (in.getVersion().before(Version.V_8_0_0)) {
            in.readOptionalString();
        }
        id = in.readString();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        version = in.readLong();
        exists = in.readBoolean();
        if (exists) {
            source = in.readBytesReference();
            if (source.length() == 0) {
                source = null;
            }
            documentFields = in.readMapValues(DocumentField::new, DocumentField::getName);
            metaFields = in.readMapValues(DocumentField::new, DocumentField::getName);
        } else {
            metaFields = Collections.emptyMap();
            documentFields = Collections.emptyMap();
        }
    }

    public GetResult(
        String index,
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        boolean exists,
        BytesReference source,
        Map<String, DocumentField> documentFields,
        Map<String, DocumentField> metaFields
    ) {
        this.index = index;
        this.id = id;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        assert (seqNo == UNASSIGNED_SEQ_NO && primaryTerm == UNASSIGNED_PRIMARY_TERM) || (seqNo >= 0 && primaryTerm >= 1)
            : "seqNo: " + seqNo + " primaryTerm: " + primaryTerm;
        assert exists || (seqNo == UNASSIGNED_SEQ_NO && primaryTerm == UNASSIGNED_PRIMARY_TERM)
            : "doc not found but seqNo/primaryTerm are set";
        this.version = version;
        this.exists = exists;
        this.source = source;
        this.documentFields = documentFields == null ? emptyMap() : documentFields;
        this.metaFields = metaFields == null ? emptyMap() : metaFields;
    }

    /**
     * Does the document exist.
     */
    public boolean isExists() {
        return exists;
    }

    /**
     * The index the document was fetched from.
     */
    public String getIndex() {
        return index;
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return id;
    }

    /**
     * The version of the doc.
     */
    public long getVersion() {
        return version;
    }

    /**
     * The sequence number assigned to the last operation that has changed this document, if found.
     */
    public long getSeqNo() {
        return seqNo;
    }

    /**
     * The primary term of the last primary that has changed this document, if found.
     */
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    /**
     * The source of the document if exists.
     */
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
     * Returns bytes reference, also un compress the source if needed.
     */
    public BytesReference sourceRef() {
        if (source == null) {
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
     * Internal source representation, might be compressed....
     */
    public BytesReference internalSourceRef() {
        return source;
    }

    /**
     * Is the source empty (not available) or not.
     */
    public boolean isSourceEmpty() {
        return source == null;
    }

    /**
     * The source of the document (as a string).
     */
    public String sourceAsString() {
        if (source == null) {
            return null;
        }
        BytesReference source = sourceRef();
        try {
            return XContentHelper.convertToJson(source, false);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to convert source to a json string");
        }
    }

    /**
     * The source of the document (As a map).
     */
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

    public Map<String, Object> getSource() {
        return sourceAsMap();
    }

    public Map<String, DocumentField> getMetadataFields() {
        return metaFields;
    }

    public Map<String, DocumentField> getDocumentFields() {
        return documentFields;
    }

    public Map<String, DocumentField> getFields() {
        Map<String, DocumentField> fields = new HashMap<>();
        fields.putAll(metaFields);
        fields.putAll(documentFields);
        return fields;
    }

    public DocumentField field(String name) {
        return getFields().get(name);
    }

    @Override
    public Iterator<DocumentField> iterator() {
        // need to join the fields and metadata fields
        Map<String, DocumentField> allFields = this.getFields();
        return allFields.values().iterator();
    }

    public XContentBuilder toXContentEmbedded(XContentBuilder builder, Params params) throws IOException {
        if (seqNo != UNASSIGNED_SEQ_NO) { // seqNo may not be assigned if read from an old node
            builder.field(_SEQ_NO, seqNo);
            builder.field(_PRIMARY_TERM, primaryTerm);
        }

        for (DocumentField field : metaFields.values()) {
            // TODO: can we avoid having an exception here?
            if (field.getName().equals(IgnoredFieldMapper.NAME)) {
                builder.field(field.getName(), field.getValues());
            } else {
                builder.field(field.getName(), field.<Object>getValue());
            }
        }

        builder.field(FOUND, exists);

        if (source != null) {
            XContentHelper.writeRawField(SourceFieldMapper.NAME, source, builder, params);
        }

        if (documentFields.isEmpty() == false) {
            builder.startObject(FIELDS);
            for (DocumentField field : documentFields.values()) {
                field.getValidValuesWriter().toXContent(builder, params);
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(_INDEX, index);
        if (builder.getRestApiVersion() == RestApiVersion.V_7) {
            builder.field(MapperService.TYPE_FIELD_NAME, MapperService.SINGLE_MAPPING_NAME);
        }
        builder.field(_ID, id);
        if (isExists()) {
            if (version != -1) {
                builder.field(_VERSION, version);
            }
            toXContentEmbedded(builder, params);
        } else {
            builder.field(FOUND, false);
        }
        builder.endObject();
        return builder;
    }

    public static GetResult fromXContentEmbedded(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        return fromXContentEmbedded(parser, null, null);
    }

    public static GetResult fromXContentEmbedded(XContentParser parser, String index, String id) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        long version = -1;
        long seqNo = UNASSIGNED_SEQ_NO;
        long primaryTerm = UNASSIGNED_PRIMARY_TERM;
        Boolean found = null;
        BytesReference source = null;
        Map<String, DocumentField> documentFields = new HashMap<>();
        Map<String, DocumentField> metaFields = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (_INDEX.equals(currentFieldName)) {
                    index = parser.text();
                } else if (parser.getRestApiVersion() == RestApiVersion.V_7 && MapperService.TYPE_FIELD_NAME.equals(currentFieldName)) {
                    deprecationLogger.compatibleCritical("mget_with_types", RestMultiGetAction.TYPES_DEPRECATION_MESSAGE);
                } else if (_ID.equals(currentFieldName)) {
                    id = parser.text();
                } else if (_VERSION.equals(currentFieldName)) {
                    version = parser.longValue();
                } else if (_SEQ_NO.equals(currentFieldName)) {
                    seqNo = parser.longValue();
                } else if (_PRIMARY_TERM.equals(currentFieldName)) {
                    primaryTerm = parser.longValue();
                } else if (FOUND.equals(currentFieldName)) {
                    found = parser.booleanValue();
                } else {
                    metaFields.put(currentFieldName, new DocumentField(currentFieldName, Collections.singletonList(parser.objectText())));
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SourceFieldMapper.NAME.equals(currentFieldName)) {
                    try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                        // the original document gets slightly modified: whitespaces or pretty printing are not preserved,
                        // it all depends on the current builder settings
                        builder.copyCurrentStructure(parser);
                        source = BytesReference.bytes(builder);
                    }
                } else if (FIELDS.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        DocumentField getField = DocumentField.fromXContent(parser);
                        documentFields.put(getField.getName(), getField);
                    }
                } else {
                    parser.skipChildren(); // skip potential inner objects for forward compatibility
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (IgnoredFieldMapper.NAME.equals(currentFieldName)) {
                    metaFields.put(currentFieldName, new DocumentField(currentFieldName, parser.list()));
                } else {
                    parser.skipChildren(); // skip potential inner arrays for forward compatibility
                }
            }
        }
        return new GetResult(index, id, seqNo, primaryTerm, version, found, source, documentFields, metaFields);
    }

    public static GetResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        return fromXContentEmbedded(parser);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeLong(version);
        out.writeBoolean(exists);
        if (exists) {
            out.writeBytesReference(source);
            out.writeMapValues(documentFields);
            out.writeMapValues(metaFields);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetResult getResult = (GetResult) o;
        return version == getResult.version
            && seqNo == getResult.seqNo
            && primaryTerm == getResult.primaryTerm
            && exists == getResult.exists
            && Objects.equals(index, getResult.index)
            && Objects.equals(id, getResult.id)
            && Objects.equals(documentFields, getResult.documentFields)
            && Objects.equals(metaFields, getResult.metaFields)
            && Objects.equals(sourceAsMap(), getResult.sourceAsMap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, seqNo, primaryTerm, exists, index, id, documentFields, metaFields, sourceAsMap());
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
