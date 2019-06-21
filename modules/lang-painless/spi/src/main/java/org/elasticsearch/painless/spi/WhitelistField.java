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

package org.elasticsearch.painless.spi;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Field represents the equivalent of a Java field available as a whitelisted class field
 * within Painless. Fields for Painless classes may be accessed exactly as fields for Java classes
 * are using the '.' operator on an existing class variable/field.
 */
public class WhitelistField {

    /** Information about where this method was whitelisted from. */
    public final String origin;

    /** The field name used to look up the field reflection object. */
    public final String fieldName;

    /** The canonical type name for the field which can be used to look up the Java field through reflection. */
    public final String canonicalTypeNameParameter;

    /** The {@link Map} of annotations for this field. */
    public final Map<Class<?>, Object> painlessAnnotations;

    /** Standard constructor.  All values must be not {@code null}. */
    public WhitelistField(String origin, String fieldName, String canonicalTypeNameParameter, List<Object> painlessAnnotations) {
        this.origin = Objects.requireNonNull(origin);
        this.fieldName = Objects.requireNonNull(fieldName);
        this.canonicalTypeNameParameter = Objects.requireNonNull(canonicalTypeNameParameter);

        if (painlessAnnotations.isEmpty()) {
            this.painlessAnnotations = Collections.emptyMap();
        } else {
            this.painlessAnnotations = Collections.unmodifiableMap(Objects.requireNonNull(painlessAnnotations).stream()
                    .map(painlessAnnotation -> new AbstractMap.SimpleEntry<>(painlessAnnotation.getClass(), painlessAnnotation))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
    }
}
