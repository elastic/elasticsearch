/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.logsdb.datageneration.FieldType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class KeywordFieldBlockLoaderTests extends BlockLoaderTestCase {
    public KeywordFieldBlockLoaderTests() {
        super(FieldType.KEYWORD);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, boolean syntheticSource) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return convert(s);
        }

        var nonNullStream = ((List<String>) value).stream().filter(Objects::nonNull);

        if ((boolean) fieldMapping.getOrDefault("doc_values", false)) {
            // Sorted and no duplicates
            return maybeFoldList(nonNullStream.collect(Collectors.toSet()).stream().sorted().map(this::convert).toList());
        }

        if ((boolean) fieldMapping.getOrDefault("store", false)) {
            return maybeFoldList(nonNullStream.map(this::convert).toList());
        }

        // Using source (either stored or synthetic).
        // Original order is preserved and values longer than ignore_above are returned.
        // TODO actual ignore_above support in data generation
        return maybeFoldList(nonNullStream.map(this::convert).toList());
    }

    private Object maybeFoldList(List<?> list) {
        if (list.isEmpty()) {
            return null;
        }

        if (list.size() == 1) {
            return list.get(0);
        }

        return list;
    }

    private BytesRef convert(String value) {
        return new BytesRef(value);
    }
}
