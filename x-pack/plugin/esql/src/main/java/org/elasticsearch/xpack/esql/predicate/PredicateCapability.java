/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import java.util.Set;

/**
 * What one source guarantees for one predicate. Producing candidates and refining a batch are independent: a source may
 * offer either, both, or neither. {@link #filtering()} applies to production. {@link #evaluation()} applies to in-place
 * refinement. A true-only producer is enough to drop rows from a {@code WHERE}. It is not enough to evaluate {@code NOT}
 * or {@code EVAL}.
 */
public record PredicateCapability(FilteringGuarantee filtering, BooleanFidelity evaluation, Set<SourceOperation> operations) {
    public PredicateCapability {
        operations = operations.isEmpty() ? Set.of() : Set.copyOf(operations);
    }

    public static PredicateCapability unsupported() {
        return new PredicateCapability(FilteringGuarantee.NONE, BooleanFidelity.NONE, Set.of());
    }

    public static PredicateCapability of(FilteringGuarantee filtering, BooleanFidelity evaluation, SourceOperation... operations) {
        return new PredicateCapability(filtering, evaluation, Set.of(operations));
    }

    public boolean producesCandidates() {
        return filtering != FilteringGuarantee.NONE && operations.contains(SourceOperation.PRODUCE);
    }

    /** In-place true, false, and null. A true-only result is not accepted here. */
    public boolean evaluatesThreeValued() {
        return evaluation == BooleanFidelity.THREE_VALUED && operations.contains(SourceOperation.REFINE);
    }
}
