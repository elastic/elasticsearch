/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A class binding represents a method call that stores state. Each class binding's Java class must
 * have exactly one public constructor and one public method excluding those inherited directly
 * from {@link Object}. The canonical type name parameters provided must match those of the
 * constructor and method combined. The constructor for a class binding's Java class will be called
 * when the binding method is called for the first time at which point state may be stored for the
 * arguments passed into the constructor. The method for a binding class will be called each time
 * the binding method is called and may use the previously stored state.
 */
public class WhitelistClassBinding {

    /** Information about where this constructor was whitelisted from. */
    public final String origin;

    /** The Java class name this class binding targets. */
    public final String targetJavaClassName;

    /** The method name for this class binding. */
    public final String methodName;

    /** The canonical type name for the return type. */
    public final String returnCanonicalTypeName;

    /**
     * A {@link List} of {@link String}s that are the Painless type names for the parameters of the
     * constructor which can be used to look up the Java constructor through reflection.
     */
    public final List<String> canonicalTypeNameParameters;

    /** The {@link Map} of annotations for this class binding. */
    public final Map<Class<?>, Object> painlessAnnotations;

    /** Standard constructor. All values must be not {@code null}. */
    public WhitelistClassBinding(
        String origin,
        String targetJavaClassName,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        List<Object> painlessAnnotations
    ) {

        this.origin = Objects.requireNonNull(origin);
        this.targetJavaClassName = Objects.requireNonNull(targetJavaClassName);

        this.methodName = Objects.requireNonNull(methodName);
        this.returnCanonicalTypeName = Objects.requireNonNull(returnCanonicalTypeName);
        this.canonicalTypeNameParameters = Objects.requireNonNull(canonicalTypeNameParameters);

        if (painlessAnnotations.isEmpty()) {
            this.painlessAnnotations = Collections.emptyMap();
        } else {
            this.painlessAnnotations = Collections.unmodifiableMap(
                Objects.requireNonNull(painlessAnnotations)
                    .stream()
                    .map(painlessAnnotation -> new AbstractMap.SimpleEntry<>(painlessAnnotation.getClass(), painlessAnnotation))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }
    }
}
