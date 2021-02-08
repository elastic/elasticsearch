/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;

import java.util.Map;

/**
 * Per-segment version of {@link SearchLookup}.
 */
public class LeafSearchLookup {

    private final LeafReaderContext ctx;
    private final LeafDocLookup docMap;
    private final SourceLookup sourceLookup;
    private final LeafStoredFieldsLookup fieldsLookup;
    private final Map<String, Object> asMap;

    public LeafSearchLookup(LeafReaderContext ctx, LeafDocLookup docMap, SourceLookup sourceLookup, LeafStoredFieldsLookup fieldsLookup) {
        this.ctx = ctx;
        this.docMap = docMap;
        this.sourceLookup = sourceLookup;
        this.fieldsLookup = fieldsLookup;
        this.asMap = Map.of(
                "doc", docMap,
                "_doc", docMap,
                "_source", sourceLookup,
                "_fields", fieldsLookup);
    }

    public Map<String, Object> asMap() {
        return this.asMap;
    }

    public SourceLookup source() {
        return this.sourceLookup;
    }

    public LeafStoredFieldsLookup fields() {
        return this.fieldsLookup;
    }

    public LeafDocLookup doc() {
        return this.docMap;
    }

    public void setDocument(int docId) {
        docMap.setDocument(docId);
        sourceLookup.setSegmentAndDocument(ctx, docId);
        fieldsLookup.setDocument(docId);
    }
}
