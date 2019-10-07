/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.index.mapper.NumberFieldMapper;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper class that defines groups of types
 */
public final class Types {

    private Types() {}

    private static final Set<String> CATEGORICAL_TYPES =
        Collections.unmodifiableSet(
            Stream.of("text", "keyword", "ip")
                .collect(Collectors.toSet()));

    private static final Set<String> NUMERICAL_TYPES =
        Collections.unmodifiableSet(
            Stream.concat(
                    Stream.of(NumberFieldMapper.NumberType.values()).map(NumberFieldMapper.NumberType::typeName),
                    Stream.of("scaled_float"))
                .collect(Collectors.toSet()));

    public static Set<String> categorical() {
        return CATEGORICAL_TYPES;
    }

    public static Set<String> numerical() {
        return NUMERICAL_TYPES;
    }
}
