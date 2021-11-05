/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * WhitelistAnnotationParser is an interface used to define how to
 * parse an annotation against any whitelist object while loading.
 */
public interface WhitelistAnnotationParser {

    Map<String, WhitelistAnnotationParser> BASE_ANNOTATION_PARSERS = Collections.unmodifiableMap(
        Stream.of(
            new AbstractMap.SimpleEntry<>(NoImportAnnotation.NAME, NoImportAnnotationParser.INSTANCE),
            new AbstractMap.SimpleEntry<>(DeprecatedAnnotation.NAME, DeprecatedAnnotationParser.INSTANCE),
            new AbstractMap.SimpleEntry<>(NonDeterministicAnnotation.NAME, NonDeterministicAnnotationParser.INSTANCE),
            new AbstractMap.SimpleEntry<>(InjectConstantAnnotation.NAME, InjectConstantAnnotationParser.INSTANCE),
            new AbstractMap.SimpleEntry<>(CompileTimeOnlyAnnotation.NAME, CompileTimeOnlyAnnotationParser.INSTANCE),
            new AbstractMap.SimpleEntry<>(AugmentedAnnotation.NAME, AugmentedAnnotationParser.INSTANCE),
            new AbstractMap.SimpleEntry<>(DynamicTypeAnnotation.NAME, DynamicTypeAnnotationParser.INSTANCE)
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
    );

    Object parse(Map<String, String> arguments);
}
