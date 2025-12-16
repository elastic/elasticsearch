/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.swisshash.SwissHashFactory;

/** A factory for constructing concrete Hash implementations. */
public class HashImplFactory {

    public static final FeatureFlag SWISS_TABLES_HASHING = new FeatureFlag("swiss_table_hashing");

    private static final SwissHashFactory SWISS_HASH_FACTORY = SWISS_TABLES_HASHING.isEnabled() ? SwissHashFactory.getInstance() : null;

    private HashImplFactory() {}

    /** Creates a new LongHashTable. */
    public static LongHashTable newLongHash(BlockFactory bf) {
        if (SWISS_HASH_FACTORY != null) {
            return SWISS_HASH_FACTORY.newLongSwissHash(bf.bigArrays().recycler(), bf.breaker());
        } else {
            return new LongHash(1, bf.bigArrays());
        }
    }

    /** Creates a new LongHashTable. */
    public static BytesRefHashTable newBytesRefHash(BlockFactory bf) {
        if (SWISS_HASH_FACTORY != null) {
            return SWISS_HASH_FACTORY.newBytesRefSwissHash(bf.bigArrays().recycler(), bf.breaker(), bf.bigArrays());
        } else {
            return new BytesRefHash(1, bf.bigArrays());
        }
    }
}
