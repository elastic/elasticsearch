/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import java.util.Comparator;

public record IndexingOperationsMemoryRequirements(long minimumRequiredHeapInBytes, long validUntil)
    implements
        Comparable<IndexingOperationsMemoryRequirements> {
    private static final Comparator<IndexingOperationsMemoryRequirements> COMPARATOR = Comparator.nullsFirst(
        Comparator.comparingLong(IndexingOperationsMemoryRequirements::minimumRequiredHeapInBytes)
            .thenComparing(IndexingOperationsMemoryRequirements::validUntil)
    );

    @Override
    public int compareTo(IndexingOperationsMemoryRequirements o) {
        return COMPARATOR.compare(this, o);
    }
}
