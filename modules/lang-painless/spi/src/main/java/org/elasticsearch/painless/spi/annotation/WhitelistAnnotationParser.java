/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
                    new AbstractMap.SimpleEntry<>(DeprecatedAnnotation.NAME, DeprecatedAnnotationParser.INSTANCE)
            ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
    );

    Object parse(Map<String, String> arguments);
}
