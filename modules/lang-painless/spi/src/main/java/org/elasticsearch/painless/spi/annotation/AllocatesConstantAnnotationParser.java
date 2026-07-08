/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.spi.annotation;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Map;

/**
 * Parses {@code @allocates_constant[bytes="40b"]}: a single {@code bytes} argument holding a non-negative
 * {@link ByteSizeValue} (a unit is required except for {@code "0"}).
 */
public class AllocatesConstantAnnotationParser implements WhitelistAnnotationParser {

    public static final String BYTES = "bytes";

    public static final AllocatesConstantAnnotationParser INSTANCE = new AllocatesConstantAnnotationParser();

    private AllocatesConstantAnnotationParser() {}

    @Override
    public Object parse(Map<String, String> arguments) {
        if (arguments.size() != 1 || arguments.containsKey(BYTES) == false) {
            throw new IllegalArgumentException("[@" + AllocatesConstantAnnotation.NAME + "] requires a single [" + BYTES + "] argument");
        }

        String value = arguments.get(BYTES).trim();
        long bytes;

        try {
            bytes = ByteSizeValue.parseBytesSizeValue(value, "[@" + AllocatesConstantAnnotation.NAME + "] [" + BYTES + "]").getBytes();
        } catch (ElasticsearchParseException epe) {
            throw new IllegalArgumentException(
                "[@" + AllocatesConstantAnnotation.NAME + "] [" + BYTES + "] argument must be a byte size value [" + value + "]",
                epe
            );
        }

        if (bytes < 0) {
            throw new IllegalArgumentException(
                "[@" + AllocatesConstantAnnotation.NAME + "] [" + BYTES + "] argument must not be negative [" + value + "]"
            );
        }

        return new AllocatesConstantAnnotation(bytes);
    }
}
