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
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

public class KeywordFieldBlockLoaderTests extends BlockLoaderTestCase {
    public KeywordFieldBlockLoaderTests(Params params) {
        super(FieldType.KEYWORD.toString(), params);
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        return expectedValue(fieldMapping, value, params, testContext);
    }

    @SuppressWarnings("unchecked")
    public static Object expectedValue(Map<String, Object> fieldMapping, Object value, Params params, TestContext testContext) {
        var nullValue = (String) fieldMapping.get("null_value");

        int ignoreAbove = fieldMapping.get("ignore_above") == null
            ? Integer.MAX_VALUE
            : ((Number) fieldMapping.get("ignore_above")).intValue();

        String normalizerName = (String) fieldMapping.get("normalizer");

        if (value == null) {
            return convert(null, nullValue, ignoreAbove, normalizerName);
        }

        if (value instanceof String s) {
            return convert(s, nullValue, ignoreAbove, normalizerName);
        }

        Function<Stream<String>, Stream<BytesRef>> convertValues = s -> s.map(v -> convert(v, nullValue, ignoreAbove, normalizerName))
            .filter(Objects::nonNull);

        boolean hasDocValues = hasDocValues(fieldMapping, true);
        boolean useDocValues = params.preference() == MappedFieldType.FieldExtractPreference.NONE
            || params.preference() == MappedFieldType.FieldExtractPreference.DOC_VALUES
            || params.syntheticSource();
        if (hasDocValues && useDocValues) {
            // Sorted and no duplicates
            var resultList = convertValues.andThen(Stream::distinct)
                .andThen(Stream::sorted)
                .andThen(Stream::toList)
                .apply(((List<String>) value).stream());
            return maybeFoldList(resultList);
        }

        // store: "true" and source
        var resultList = convertValues.andThen(Stream::toList).apply(((List<String>) value).stream());
        return maybeFoldList(resultList);
    }

    private static BytesRef convert(String value, String nullValue, int ignoreAbove, String normalizer) {
        if (value == null) {
            if (nullValue != null) {
                value = nullValue;
            } else {
                return null;
            }
        }
        if (Objects.equals(normalizer, "lowercase")) {
            // hopefully not Turkish...
            value = value.toLowerCase(Locale.ROOT);
        } else if (normalizer != null) {
            // we probably can't get here anyway, since MapperServiceTestCase only initializes the lowercase normalizer
            throw new IllegalArgumentException("normalizer [" + normalizer + "] not supported for block loader tests");
        }
        return value.length() <= ignoreAbove ? new BytesRef(value) : null;
    }
}
