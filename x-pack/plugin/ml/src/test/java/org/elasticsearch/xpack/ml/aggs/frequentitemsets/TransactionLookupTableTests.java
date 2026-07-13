/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

public class TransactionLookupTableTests extends ESTestCase {

    static BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    public void testBasic() {
        try (TransactionsLookupTable transactions = new TransactionsLookupTable(10, mockBigArrays())) {

            // setup 3 transactions
            ItemSetBitSet set = new ItemSetBitSet();
            set.set(0);
            set.set(3);
            set.set(200);
            set.set(5);
            set.set(65);
            transactions.append(set);
            set.clear();
            set.set(2);
            set.set(33);
            set.set(44);
            transactions.append(set);
            assertEquals(2, transactions.size());
            set.clear();
            set.set(3);
            set.set(5);
            set.set(65);
            set.set(99);
            transactions.append(set);
            assertEquals(3, transactions.size());

            // lookup
            set.clear();
            set.set(3);
            set.set(65);
            assertTrue(transactions.isSubsetOf(0, set));
            assertFalse(transactions.isSubsetOf(1, set));
            assertTrue(transactions.isSubsetOf(2, set));

            set.set(64);
            assertFalse(transactions.isSubsetOf(0, set));
            assertFalse(transactions.isSubsetOf(1, set));
            assertFalse(transactions.isSubsetOf(2, set));
            set.clear(64);

            set.set(258);
            assertFalse(transactions.isSubsetOf(0, set));
            assertFalse(transactions.isSubsetOf(1, set));
            assertFalse(transactions.isSubsetOf(2, set));
            set.clear(258);

            set.set(400);
            assertFalse(transactions.isSubsetOf(0, set));
            assertFalse(transactions.isSubsetOf(1, set));
            assertFalse(transactions.isSubsetOf(2, set));
            set.clear(400);

            set.set(99);
            assertFalse(transactions.isSubsetOf(0, set));
            assertFalse(transactions.isSubsetOf(1, set));
            assertTrue(transactions.isSubsetOf(2, set));
        }
    }

}
