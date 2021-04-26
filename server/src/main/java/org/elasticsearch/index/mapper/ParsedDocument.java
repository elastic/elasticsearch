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
import org.elasticsearch.index.mapper.ParseContext.Document;

import java.util.List;

/**
 * The result of parsing a document.
 */
public class ParsedDocument {

    private final Field version;

    private final String id;
    private final SeqNoFieldMapper.SequenceIDFields seqID;

    private final String routing;

    private final List<Document> documents;

    private BytesReference source;
    private XContentType xContentType;

    private Mapping dynamicMappingsUpdate;

    public static ParsedDocument noopTombstone(String index, String reason) {
        ParsedDocument doc = deleteTombstone(index, null);
        // Store the reason of a noop as a raw string in the _source field
        final BytesRef byteRef = new BytesRef(reason);
        doc.rootDoc().add(new StoredField(SourceFieldMapper.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
        return doc;
    }

    public static ParsedDocument deleteTombstone(String index, String id) {
        SourceToParse source = new SourceToParse(index, id == null ? "" : id, new BytesArray("{}"), XContentType.JSON);
        ParseContext.InternalParseContext context = new ParseContext.InternalParseContext(null, 1,null, source, null);
        if (id != null) {
            IdFieldMapper.NO_FIELDDATA_INSTANCE.preParse(context);
        }
        VersionFieldMapper.INSTANCE.preParse(context);
        SeqNoFieldMapper.INSTANCE.preParse(context);
        ParsedDocument doc = new ParsedDocument(
            context.version(),
            context.seqID(),
            context.sourceToParse().id(),
            source.routing(),
            context.docs(),
            context.sourceToParse().source(),
            context.sourceToParse().getXContentType(),
            null
        );
        doc.seqID.tombstoneField.setLongValue(1);
        doc.rootDoc().add(doc.seqID.tombstoneField);
        return doc;
    }

    public ParsedDocument(Field version,
                          SeqNoFieldMapper.SequenceIDFields seqID,
                          String id,
                          String routing,
                          List<Document> documents,
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

    public Document rootDoc() {
        return documents.get(documents.size() - 1);
    }

    public List<Document> docs() {
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
