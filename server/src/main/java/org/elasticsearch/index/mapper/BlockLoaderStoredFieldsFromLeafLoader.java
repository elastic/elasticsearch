/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BlockLoaderStoredFieldsFromLeafLoader implements BlockLoader.StoredFields {
    private final LeafStoredFieldLoader loader;
    private final SourceLoader.Leaf sourceLoader;
    private Source source;
    private int docId = -1;
    private int loaderDocId = -1;
    private int sourceDocId = -1;

    public BlockLoaderStoredFieldsFromLeafLoader(LeafStoredFieldLoader loader, SourceLoader.Leaf sourceLoader) {
        this.loader = loader;
        this.sourceLoader = sourceLoader;
    }

    public void advanceTo(int docId) {
        this.docId = docId;
    }

    private void advanceIfNeeded() throws IOException {
        if (loaderDocId != docId) {
            if (loader != null) {
                loader.advanceTo(docId);
            }
            loaderDocId = docId;
        }
    }

    @Override
    public Source source() throws IOException {
        advanceIfNeeded();
        if (sourceLoader != null) {
            if (sourceDocId != docId) {
                source = sourceLoader.source(loader, docId);
                sourceDocId = docId;
            }
        }
        return source;
    }

    @Override
    public String id() throws IOException {
        advanceIfNeeded();
        return loader.id();
    }

    @Override
    public String routing() throws IOException {
        advanceIfNeeded();
        return loader.routing();
    }

    @Override
    public Map<String, List<Object>> storedFields() throws IOException {
        advanceIfNeeded();
        return loader.storedFields();
    }

    @Override
    public boolean loaded() {
        return loaderDocId == docId;
    }

    /**
     * Returns the raw byte size of the last loaded _source. This is useful for estimating
     * the untracked memory overhead from source parsing (parsed Java Strings occupy
     * approximately 2x the raw byte size due to UTF-16 encoding).
     */
    public long lastSourceBytesSize() {
        return source != null && source.internalSourceRef() != null ? source.internalSourceRef().length() : 0;
    }

    /**
     * Releases the cached parsed source to allow immediate garbage collection of
     * large String objects created during source parsing. For a 5MB text field,
     * the parsed Java String is ~10MB (UTF-16). Releasing it eagerly prevents
     * accumulation of untracked memory that can cause OOM.
     */
    public void releaseParsedSource() {
        source = null;
        sourceDocId = -1;
    }
}
