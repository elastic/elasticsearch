/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.swisshash.SwissHashFactory;

/**
 * A factory for constructing concrete Hash implementations.
 *
 * <p> The specific implementation returned by this factory is chosen once at
 * class initialization time and remains fixed for the lifetime of the JVM. All
 * factory methods will therefore consistently return instances of the same
 * concrete implementation type.
 */
public class HashImplFactory {

    private static final SwissHashFactory SWISS_HASH_FACTORY = SwissHashFactory.getInstance();
    public static final boolean SWISS_HASH_AVAILABLE = SWISS_HASH_FACTORY != null;

    private HashImplFactory() {}

    /** Creates a new LongHashTable. */
    public static LongHashTable newLongHash(BlockFactory bf) {
        if (SWISS_HASH_FACTORY != null) {
            return SWISS_HASH_FACTORY.newLongSwissHash(bf.bigArrays().recycler(), bf.breaker());
        } else {
            return new LongHash(1, bf.bigArrays());
        }
    }

    /**
     * Creates a new {@link LongHashTable} using only a {@link BigArrays} for memory accounting.
     * Used by aggregator state classes that don't have a {@link BlockFactory} in scope.
     */
    public static LongHashTable newLongHash(BigArrays bigArrays) {
        if (SWISS_HASH_FACTORY != null) {
            CircuitBreaker breaker = bigArrays.breakerService() != null
                ? bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST)
                : new NoopCircuitBreaker(CircuitBreaker.REQUEST);
            return SWISS_HASH_FACTORY.newLongSwissHash(bigArrays.recycler(), breaker);
        } else {
            return new LongHash(1, bigArrays);
        }
    }

    /** Creates a new LongLongHashTable. */
    public static LongLongHashTable newLongLongHash(BlockFactory bf) {
        if (SWISS_HASH_FACTORY != null) {
            return SWISS_HASH_FACTORY.newLongLongSwissHash(bf.bigArrays().recycler(), bf.breaker());
        } else {
            return new LongLongHash(1, bf.bigArrays());
        }
    }

    /** Creates a new BytesRefHashTable. */
    public static BytesRefHashTable newBytesRefHash(BlockFactory bf) {
        if (SWISS_HASH_FACTORY != null) {
            return SWISS_HASH_FACTORY.newBytesRefSwissHash(bf.bigArrays().recycler(), bf.breaker(), bf.bigArrays());
        } else {
            return new BytesRefHash(1, bf.bigArrays());
        }
    }
}
