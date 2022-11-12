/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Per-segment version of {@link SearchLookup}.
 */
public class LeafSearchLookup {

    private int doc;
    private final LeafDocLookup docMap;
    private final LeafStoredFieldsLookup fieldsLookup;
    private final Map<String, Object> asMap;
    private final Supplier<Source> source;

    public LeafSearchLookup(
        LeafReaderContext ctx,
        LeafDocLookup docMap,
        SourceProvider sourceProvider,
        LeafStoredFieldsLookup fieldsLookup
    ) {
        this.docMap = docMap;
        this.fieldsLookup = fieldsLookup;
        this.source = () -> {
            try {
                return sourceProvider.getSource(ctx, doc);
            } catch (IOException e) {
                throw new UncheckedIOException("Couldn't load source", e);
            }
        };
        this.asMap = Map.of("doc", docMap, "_doc", docMap, "_source", source, "_fields", fieldsLookup);
    }

    public Map<String, Object> asMap() {
        return this.asMap;
    }

    public LeafStoredFieldsLookup fields() {
        return this.fieldsLookup;
    }

    public LeafDocLookup doc() {
        return this.docMap;
    }

    public Supplier<Source> source() {
        return this.source;
    }

    public void setDocument(int docId) {
        this.doc = docId;
        docMap.setDocument(docId);
        fieldsLookup.setDocument(docId);
    }
}
