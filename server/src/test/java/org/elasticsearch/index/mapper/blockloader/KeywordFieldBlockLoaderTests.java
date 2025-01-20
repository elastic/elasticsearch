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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

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

        var ignoreAbove = fieldMapping.get("ignore_above") == null
            ? Integer.MAX_VALUE
            : ((Number) fieldMapping.get("ignore_above")).intValue();

        if (value instanceof String s) {
            return convert(s, ignoreAbove);
        }

        Function<Stream<String>, Stream<BytesRef>> convertValues = s -> s.map(v -> convert(v, ignoreAbove)).filter(Objects::nonNull);

        if ((boolean) fieldMapping.getOrDefault("doc_values", false)) {
            // Sorted and no duplicates

            var values = new HashSet<>((List<String>) value);
            var resultList = convertValues.compose(s -> values.stream().filter(Objects::nonNull).sorted())
                .andThen(Stream::toList)
                .apply(values.stream());
            return maybeFoldList(resultList);
        }

        // store: "true" and source
        var resultList = convertValues.andThen(Stream::toList).apply(((List<String>) value).stream());
        return maybeFoldList(resultList);
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

    private BytesRef convert(String value, int ignoreAbove) {
        if (value == null) {
            return null;
        }

        return value.length() <= ignoreAbove ? new BytesRef(value) : null;
    }
}
