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

        var resultList = ((List<String>) value).stream()
            .map(s -> convert(s, nullValue, ignoreAbove))
            .filter(Objects::nonNull)
            .distinct()
            .sorted()
            .toList();
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
