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
 * Class represents the equivalent of a Java class in Painless complete with super classes,
 * constructors, methods, and fields. There must be a one-to-one mapping of class names to Java
 * classes. Though, since multiple whitelists may be combined into a single whitelist for a
 * specific context, as long as multiple classes representing the same Java class have the same
 * class name and have legal constructor/method overloading they can be merged together.
 *
 * Classes in Painless allow for arity overloading for constructors and methods. Arity overloading
 * means that multiple constructors are allowed for a single class as long as they have a different
 * number of parameters, and multiples methods with the same name are allowed for a single class
 * as long as they have the same return type and a different number of parameters.
 *
 * Classes will automatically extend other whitelisted classes if the Java class they represent is a
 * subclass of other classes including Java interfaces.
 */
public final class WhitelistClass {

    /** Information about where this class was white-listed from. */
    public final String origin;

    /** The Java class name this class represents. */
    public final String javaClassName;

    /** The {@link List} of whitelisted ({@link WhitelistConstructor}s) available to this class. */
    public final List<WhitelistConstructor> whitelistConstructors;

    /** The {@link List} of whitelisted ({@link WhitelistMethod}s) available to this class. */
    public final List<WhitelistMethod> whitelistMethods;

    /** The {@link List} of whitelisted ({@link WhitelistField}s) available to this class. */
    public final List<WhitelistField> whitelistFields;

    /** The {@link Map} of annotations for this class. */
    public final Map<Class<?>, Object> painlessAnnotations;

    /** Standard constructor. All values must be not {@code null}. */
    public WhitelistClass(String origin, String javaClassName,
            List<WhitelistConstructor> whitelistConstructors, List<WhitelistMethod> whitelistMethods, List<WhitelistField> whitelistFields,
            List<Object> painlessAnnotations) {

        this.origin = Objects.requireNonNull(origin);
        this.javaClassName = Objects.requireNonNull(javaClassName);

        this.whitelistConstructors = Collections.unmodifiableList(Objects.requireNonNull(whitelistConstructors));
        this.whitelistMethods = Collections.unmodifiableList(Objects.requireNonNull(whitelistMethods));
        this.whitelistFields = Collections.unmodifiableList(Objects.requireNonNull(whitelistFields));

        if (painlessAnnotations.isEmpty()) {
            this.painlessAnnotations = Collections.emptyMap();
        } else {
            this.painlessAnnotations = Collections.unmodifiableMap(Objects.requireNonNull(painlessAnnotations).stream()
                    .map(painlessAnnotation -> new AbstractMap.SimpleEntry<>(painlessAnnotation.getClass(), painlessAnnotation))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
    }
}
