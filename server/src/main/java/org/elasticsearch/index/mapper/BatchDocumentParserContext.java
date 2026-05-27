/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Collections;

/**
 * Minimal {@link DocumentParserContext} used by the bulk batch-indexing fast path. It holds a
 * single {@link LuceneDocument} (no nested documents), a mutable {@link XContentParser}
 * reference that callers reposition per field, and an empty {@link ContentPath}. Tracking state
 * (ignored fields, version, seqID, routing, etc.) is inherited from {@link DocumentParserContext}.
 *
 * <p>The batch path that uses this context resolves mappers up front and only drives plain
 * scalar fields (see {@code ShardBatchMapper}), so behaviors that require nested documents,
 * array handling, or dot expansion are not exercised here.
 */
final class BatchDocumentParserContext extends DocumentParserContext {

    // TODO: Will need to implement ContentPath for future mappers.
    private final ContentPath path = new ContentPath();
    private final LuceneDocument document = new LuceneDocument();
    private final BytesRef tsid;
    private XContentParser parser;

    BatchDocumentParserContext(MappingLookup mappingLookup, MappingParserContext mappingParserContext, SourceToParse sourceToParse) {
        super(
            mappingLookup,
            mappingParserContext,
            sourceToParse,
            mappingLookup.getMapping().getRoot(),
            ObjectMapper.Dynamic.getRootDynamic(mappingLookup)
        );
        this.tsid = sourceToParse.tsid();
    }

    void setParser(XContentParser parser) {
        this.parser = parser;
    }

    @Override
    public ContentPath path() {
        return path;
    }

    @Override
    public XContentParser parser() {
        if (parser == null) {
            throw new IllegalStateException("XContentParser is not available outside the column-parsing phase of the batch path");
        }
        return parser;
    }

    @Override
    public LuceneDocument doc() {
        return document;
    }

    @Override
    public LuceneDocument rootDoc() {
        return document;
    }

    @Override
    protected void addDoc(LuceneDocument doc) {
        throw new UnsupportedOperationException("batch indexing path does not support nested documents");
    }

    @Override
    public Iterable<LuceneDocument> nonRootDocuments() {
        return Collections.emptyList();
    }

    @Override
    public BytesRef getTsid() {
        return tsid;
    }
}
