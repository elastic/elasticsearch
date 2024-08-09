/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.injector.spec;

import java.util.stream.Stream;

/**
 * Indicates that there are multiple ways to inject a given type.
 *
 * <p>
 * This is an error only if some type attempts to inject {@link #requestedType()},
 * because there is no single best object of that type.
 *
 * <p>
 * This is arranged as a tree so it can be instantiated in constant time;
 * if there are more than two candidates, than {@link #option1} and/or {@link #option2}
 * will themselves be an {@link AmbiguousSpec}.
 */
public record AmbiguousSpec(Class<?> requestedType, InjectionSpec option1, InjectionSpec option2) implements InjectionSpec {
    public Stream<UnambiguousSpec> candidates() {
        return specStream(this);
    }

    private static Stream<UnambiguousSpec> specStream(InjectionSpec spec) {
        if (spec instanceof AmbiguousSpec a) {
            return Stream.concat(specStream(a.option1()), specStream(a.option2()));
        } else {
            return Stream.of((UnambiguousSpec) spec);
        }
    }

    @Override
    public String toString() {
        return "AmbiguousSpec{" +
            "requestedType=" + requestedType +
            ", some options: " + candidates().limit(4).toList() +
            '}';
    }
}
