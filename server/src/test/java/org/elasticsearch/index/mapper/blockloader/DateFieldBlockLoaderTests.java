/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class DateFieldBlockLoaderTests extends BlockLoaderTestCase {
    public DateFieldBlockLoaderTests(Params params) {
        super(FieldType.DATE.toString(), params);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        var format = (String) fieldMapping.get("format");
        var nullValue = fieldMapping.get("null_value") != null ? format(fieldMapping.get("null_value"), format) : null;

        if (value instanceof List<?> == false) {
            return convert(value, nullValue, format);
        }

        if ((boolean) fieldMapping.getOrDefault("doc_values", false)) {
            // Sorted
            var resultList = ((List<Object>) value).stream()
                .map(v -> convert(v, nullValue, format))
                .filter(Objects::nonNull)
                .sorted()
                .toList();
            return maybeFoldList(resultList);
        }

        // parsing from source, not sorted
        var resultList = ((List<Object>) value).stream().map(v -> convert(v, nullValue, format)).filter(Objects::nonNull).toList();
        return maybeFoldList(resultList);
    }

    private Long convert(Object value, Long nullValue, String format) {
        if (value == null) {
            return nullValue;
        }

        return format(value, format);
    }

    private Long format(Object value, String format) {
        if (format == null) {
            if (value == null) {
                return null;
            }
            if (value instanceof Integer i) {
                return i.longValue();
            }
            if (value instanceof Long l) {
                return l;
            }
            if (value instanceof String s) {
                try {
                    return Instant.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(s)).toEpochMilli();
                } catch (Exception e) {
                    // malformed
                    return null;
                }
            }

            throw new IllegalStateException("Unexpected value: " + value);
        }

        try {
            return Instant.from(
                DateTimeFormatter.ofPattern(format, Locale.ROOT).withZone(ZoneId.from(ZoneOffset.UTC)).parse((String) value)
            ).toEpochMilli();
        } catch (Exception e) {
            // malformed
            return null;
        }
    }
}
