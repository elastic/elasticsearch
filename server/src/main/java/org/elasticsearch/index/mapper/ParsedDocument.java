/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.util.Collections;
import java.util.List;

/**
 * The result of parsing a document.
 */
public class ParsedDocument {

    private final Field version;

    private final String id;
    private final SeqNoFieldMapper.SequenceIDFields seqID;

    private final String routing;

    private final List<LuceneDocument> documents;

    private BytesReference source;
    private XContentType xContentType;

    private Mapping dynamicMappingsUpdate;

    /**
     * Create a no-op tombstone document
     * @param reason    the reason for the no-op
     */
    public static ParsedDocument noopTombstone(String reason) {
        LuceneDocument document = new LuceneDocument();
        SeqNoFieldMapper.SequenceIDFields seqIdFields = SeqNoFieldMapper.SequenceIDFields.tombstone();
        seqIdFields.addFields(document);
        Field versionField = VersionFieldMapper.versionField();
        document.add(versionField);
        // Store the reason of a noop as a raw string in the _source field
        final BytesRef byteRef = new BytesRef(reason);
        document.add(new StoredField(SourceFieldMapper.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
        return new ParsedDocument(
            versionField,
            seqIdFields,
            "",
            null,
            Collections.singletonList(document),
            new BytesArray("{}"),
            XContentType.JSON,
            null
        );
    }

    /**
     * Create a delete tombstone document, which will be used in soft-update methods.
     * The returned document consists only _uid, _seqno, _term and _version fields; other metadata fields are excluded.
     * @param id    the id of the deleted document
     */
    public static ParsedDocument deleteTombstone(String id) {
        LuceneDocument document = new LuceneDocument();
        SeqNoFieldMapper.SequenceIDFields seqIdFields = SeqNoFieldMapper.SequenceIDFields.tombstone();
        seqIdFields.addFields(document);
        Field versionField = VersionFieldMapper.versionField();
        document.add(versionField);
        document.add(IdFieldMapper.idField(id));
        return new ParsedDocument(
            versionField,
            seqIdFields,
            id,
            null,
            Collections.singletonList(document),
            new BytesArray("{}"),
            XContentType.JSON,
            null
        );
    }

    public ParsedDocument(Field version,
                          SeqNoFieldMapper.SequenceIDFields seqID,
                          String id,
                          String routing,
                          List<LuceneDocument> documents,
                          BytesReference source,
                          XContentType xContentType,
                          Mapping dynamicMappingsUpdate) {
        this.version = version;
        this.seqID = seqID;
        this.id = id;
        this.routing = routing;
        this.documents = documents;
        this.source = source;
        this.dynamicMappingsUpdate = dynamicMappingsUpdate;
        this.xContentType = xContentType;
    }

    public String id() {
        return this.id;
    }

    public Field version() {
        return version;
    }

    public void updateSeqID(long sequenceNumber, long primaryTerm) {
        this.seqID.seqNo.setLongValue(sequenceNumber);
        this.seqID.seqNoDocValue.setLongValue(sequenceNumber);
        this.seqID.primaryTerm.setLongValue(primaryTerm);
    }

    public String routing() {
        return this.routing;
    }

    public LuceneDocument rootDoc() {
        return documents.get(documents.size() - 1);
    }

    public List<LuceneDocument> docs() {
        return this.documents;
    }

    public BytesReference source() {
        return this.source;
    }

    public XContentType getXContentType() {
        return this.xContentType;
    }

    public void setSource(BytesReference source, XContentType xContentType) {
        this.source = source;
        this.xContentType = xContentType;
    }

    /**
     * Return dynamic updates to mappings or {@code null} if there were no
     * updates to the mappings.
     */
    public Mapping dynamicMappingsUpdate() {
        return dynamicMappingsUpdate;
    }

    public void addDynamicMappingsUpdate(Mapping update) {
        if (dynamicMappingsUpdate == null) {
            dynamicMappingsUpdate = update;
        } else {
            dynamicMappingsUpdate = dynamicMappingsUpdate.merge(update, MergeReason.MAPPING_UPDATE);
        }
    }

    @Override
    public String toString() {
        return "Document id[" + id + "] doc [" + documents + ']';
    }

}
