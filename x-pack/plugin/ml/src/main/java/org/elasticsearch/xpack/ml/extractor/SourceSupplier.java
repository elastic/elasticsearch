/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;

import java.util.Map;
import java.util.function.Supplier;

/**
 * A supplier for the source of a search hit with caching capabilities.
 */
public final class SourceSupplier implements Supplier<Map<String, Object>> {

    private final SearchHit searchHit;
    private Map<String, Object> sourceMap;

    public SourceSupplier(SearchHit searchHit) {
        this.searchHit = searchHit;
    }

    @Override
    public Map<String, Object> get() {
        if (sourceMap == null) {
            sourceMap = searchHit.getSourceAsMap();
        }
        return sourceMap;
    }
}
