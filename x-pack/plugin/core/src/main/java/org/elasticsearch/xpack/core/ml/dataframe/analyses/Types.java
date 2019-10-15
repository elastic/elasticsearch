/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.TextFieldMapper;

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
        Stream.of(TextFieldMapper.CONTENT_TYPE, KeywordFieldMapper.CONTENT_TYPE, IpFieldMapper.CONTENT_TYPE)
            .collect(Collectors.toUnmodifiableSet());

    private static final Set<String> NUMERICAL_TYPES =
        Stream.concat(Stream.of(NumberType.values()).map(NumberType::typeName), Stream.of("scaled_float"))
            .collect(Collectors.toUnmodifiableSet());

    private static final Set<String> DISCRETE_NUMERICAL_TYPES =
        Stream.of(NumberType.BYTE, NumberType.SHORT, NumberType.INTEGER, NumberType.LONG)
            .map(NumberType::typeName)
            .collect(Collectors.toUnmodifiableSet());

    private static final Set<String> BOOL_TYPES = Collections.singleton(BooleanFieldMapper.CONTENT_TYPE);

    public static Set<String> categorical() {
        return CATEGORICAL_TYPES;
    }

    public static Set<String> numerical() {
        return NUMERICAL_TYPES;
    }

    public static Set<String> discreteNumerical() {
        return DISCRETE_NUMERICAL_TYPES;
    }

    public static Set<String> bool() {
        return BOOL_TYPES;
    }
}
