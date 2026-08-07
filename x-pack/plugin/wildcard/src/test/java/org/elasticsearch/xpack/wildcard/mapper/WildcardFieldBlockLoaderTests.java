/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

public class WildcardFieldBlockLoaderTests extends BlockLoaderTestCase {
    public WildcardFieldBlockLoaderTests(Params params) {
        super(FieldType.WILDCARD.toString(), params);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        var nullValue = (String) fieldMapping.get("null_value");

        var ignoreAbove = fieldMapping.get("ignore_above") == null
            ? Integer.MAX_VALUE
            : ((Number) fieldMapping.get("ignore_above")).intValue();

        if (value == null) {
            return convert(null, nullValue, ignoreAbove);
        }

        if (value instanceof String s) {
            return convert(s, nullValue, ignoreAbove);
        }

        Function<Stream<String>, Stream<BytesRef>> convertValues = s -> s.map(v -> convert(v, nullValue, ignoreAbove))
            .filter(Objects::nonNull);

        // Strictly columnar index modes store values in document order, keeping duplicates, instead of sorting and deduplicating them.
        boolean preserveOrder = params.indexMode().isColumnar();
        var resultList = preserveOrder
            ? convertValues.andThen(Stream::toList).apply(((List<String>) value).stream())
            : convertValues.andThen(Stream::distinct)
                .andThen(Stream::sorted)
                .andThen(Stream::toList)
                .apply(((List<String>) value).stream());
        return maybeFoldList(resultList);
    }

    private static BytesRef convert(String value, String nullValue, int ignoreAbove) {
        if (value == null) {
            if (nullValue != null) {
                value = nullValue;
            } else {
                return null;
            }
        }

        return value.length() <= ignoreAbove ? new BytesRef(value) : null;
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singleton(new Wildcard());
    }
}
