/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.test;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to build {@link SearchHit} in tests
 */
public class SearchHitBuilder {

    private final SearchHit hit;
    private final Map<String, DocumentField> fields;

    public SearchHitBuilder(int docId) {
        fields = new HashMap<>();
        hit = new SearchHit(docId, null, fields, null);
    }

    public SearchHitBuilder addField(String name, Object value) {
        return addField(name, Arrays.asList(value));
    }

    public SearchHitBuilder addField(String name, List<Object> values) {
        fields.put(name, new DocumentField(name, values));
        return this;
    }

    public SearchHitBuilder setSource(String sourceJson) {
        hit.sourceRef(new BytesArray(sourceJson));
        return this;
    }

    public SearchHitBuilder setLongSortValue(Long sortValue) {
        hit.sortValues(new Long[] { sortValue }, new DocValueFormat[] { DocValueFormat.RAW });
        return this;
    }

    public SearchHit build() {
        return hit;
    }
}
