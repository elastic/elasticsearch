/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    public BlockLoaderStoredFieldsFromLeafLoader(LeafStoredFieldLoader loader, SourceLoader.Leaf sourceLoader) {
        this.loader = loader;
        this.sourceLoader = sourceLoader;
    }

    public void advanceTo(int doc) throws IOException {
        loader.advanceTo(doc);
        if (sourceLoader != null) {
            source = sourceLoader.source(loader, doc);
        }
    }

    @Override
    public Source source() {
        return source;
    }

    @Override
    public String id() {
        return loader.id();
    }

    @Override
    public String routing() {
        return loader.routing();
    }

    @Override
    public Map<String, List<Object>> storedFields() {
        return loader.storedFields();
    }
}
