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
 * Constructor represents the equivalent of a Java constructor available as a whitelisted class
 * constructor within Painless. Constructors for Painless classes may be accessed exactly as
 * constructors for Java classes are using the 'new' keyword. Painless classes may have multiple
 * constructors as long as they comply with arity overloading described for {@link WhitelistClass}.
 */
public final class WhitelistConstructor {

    /** Information about where this constructor was whitelisted from. */
    public final String origin;

    /**
     * A {@link List} of {@link String}s that are the Painless type names for the parameters of the
     * constructor which can be used to look up the Java constructor through reflection.
     */
    public final List<String> canonicalTypeNameParameters;

    /** The {@link Map} of annotations for this constructor. */
    public final Map<Class<?>, Object> painlessAnnotations;

    /** Standard constructor. All values must be not {@code null}. */
    public WhitelistConstructor(String origin, List<String> canonicalTypeNameParameters, List<Object> painlessAnnotations) {
        this.origin = Objects.requireNonNull(origin);
        this.canonicalTypeNameParameters = Collections.unmodifiableList(Objects.requireNonNull(canonicalTypeNameParameters));

        if (painlessAnnotations.isEmpty()) {
            this.painlessAnnotations = Collections.emptyMap();
        } else {
            this.painlessAnnotations = Collections.unmodifiableMap(Objects.requireNonNull(painlessAnnotations).stream()
                    .map(painlessAnnotation -> new AbstractMap.SimpleEntry<>(painlessAnnotation.getClass(), painlessAnnotation))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
    }
}
