/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.injection.spec;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Objects;

/**
 * Indicates that a type should be instantiated by calling the given {@link java.lang.invoke.MethodHandle}.
 * <p>
 * <em>Design note</em>: the intent is that the semantics are fully specified by this record,
 * and no additional reflection logic is required to determine how the object should be injected.
 * Roughly speaking: all the reflection should be finished, and the results should be stored in this object.
 */
public record MethodHandleSpec(Class<?> requestedType, MethodHandle methodHandle, List<ParameterSpec> parameters) implements InjectionSpec {
    public MethodHandleSpec {
        assert Objects.equals(methodHandle.type().parameterList(), parameters.stream().map(ParameterSpec::formalType).toList())
            : "MethodHandle parameter types must match the supplied parameter info; "
                + methodHandle.type().parameterList()
                + " vs "
                + parameters;
    }
}
