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
 * Method represents the equivalent of a Java method available as a whitelisted class method
 * within Painless. Methods for Painless classes may be accessed exactly as methods for Java classes
 * are using the '.' operator on an existing class variable/field. Painless classes may have multiple
 * methods with the same name as long as they comply with arity overloading described in
 * {@link WhitelistClass}.
 *
 * Classes may also have additional methods that are not part of the Java class the class represents -
 * these are known as augmented methods. An augmented method can be added to a class as a part of any
 * Java class as long as the method is static and the first parameter of the method is the Java class
 * represented by the class. Note that the augmented method's parent Java class does not need to be
 * whitelisted.
 */
public class WhitelistMethod {

    /** Information about where this method was whitelisted from. */
    public final String origin;

    /**
     * The class name for the owner of an augmented method. If the method is not augmented
     * this should be {@code null}.
     */
    public final String augmentedCanonicalClassName;

    /** The method name used to look up the method reflection object. */
    public final String methodName;

    /**
     * The canonical type name for the return type.
     */
    public final String returnCanonicalTypeName;

    /**
     * A {@link List} of {@link String}s that are the canonical type names for the parameters of the
     * method used to look up the method reflection object.
     */
    public final List<String> canonicalTypeNameParameters;

    /** The {@link Map} of annotations for this method. */
    public final Map<Class<?>, Object> painlessAnnotations;

    /**
     * Standard constructor. All values must be not {@code null} with the exception of
     * augmentedCanonicalClassName; augmentedCanonicalClassName will be {@code null} unless the method
     * is augmented as described in the class documentation.
     */
    public WhitelistMethod(String origin, String augmentedCanonicalClassName, String methodName,
            String returnCanonicalTypeName, List<String> canonicalTypeNameParameters,
            List<Object> painlessAnnotations) {

        this.origin = Objects.requireNonNull(origin);
        this.augmentedCanonicalClassName = augmentedCanonicalClassName;
        this.methodName = methodName;
        this.returnCanonicalTypeName = Objects.requireNonNull(returnCanonicalTypeName);
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
