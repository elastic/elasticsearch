/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import java.util.EnumSet;
import java.util.Set;

/** What one source guarantees for one predicate, and which operations it offers. */
public record PredicateCapability(PredicateSupport support, Set<SourceOperation> operations) {
    public PredicateCapability {
        operations = operations.isEmpty() ? Set.of() : Set.copyOf(operations);
    }

    public static PredicateCapability unsupported() {
        return new PredicateCapability(PredicateSupport.UNSUPPORTED, Set.of());
    }

    public static PredicateCapability of(PredicateSupport support, SourceOperation... operations) {
        return new PredicateCapability(support, EnumSet.copyOf(java.util.Arrays.asList(operations)));
    }

    public boolean produces() {
        return support != PredicateSupport.UNSUPPORTED && operations.contains(SourceOperation.PRODUCE);
    }

    public boolean refines() {
        return support == PredicateSupport.EXACT && operations.contains(SourceOperation.REFINE);
    }
}
