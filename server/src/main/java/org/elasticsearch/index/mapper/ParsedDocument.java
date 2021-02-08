/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

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

    /**
     * Makes the processing document as a tombstone document rather than a regular document.
     * Tombstone documents are stored in Lucene index to represent delete operations or Noops.
     */
    ParsedDocument toTombstone() {
        assert docs().size() == 1 : "Tombstone should have a single doc [" + docs() + "]";
        this.seqID.tombstoneField.setLongValue(1);
        rootDoc().add(this.seqID.tombstoneField);
        return this;
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
