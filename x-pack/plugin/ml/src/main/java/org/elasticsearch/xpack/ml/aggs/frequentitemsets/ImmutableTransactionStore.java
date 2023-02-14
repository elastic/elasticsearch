/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * Simplified, immutable version of transaction store
 *
 */
public final class ImmutableTransactionStore extends TransactionStore {

    private final BytesRefArray items;
    private final LongArray itemCounts;
    private final long totalItemCount;
    private final BytesRefArray transactions;
    private final LongArray transactionCounts;
    private final long totalTransactionCount;
    private final long filteredTransactionCount;

    // base size of sealed transaction store, update if you add classes
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ImmutableTransactionStore.class) + 2
        * RamUsageEstimator.shallowSizeOfInstance(BytesRefArray.class) + 2 * RamUsageEstimator.shallowSizeOfInstance(LongArray.class);

    // internal constructor for {@link HashBasedTransactionStore} that takes over the ownership of BytesRefArray
    ImmutableTransactionStore(
        BigArrays bigArrays,
        BytesRefArray items,
        LongArray itemCounts,
        long totalItemCount,
        BytesRefArray transactions,
        LongArray transactionCounts,
        long totalTransactionCount,
        long filteredTransactionCount
    ) {
        super(bigArrays);

        this.items = items;
        this.itemCounts = itemCounts;
        this.totalItemCount = totalItemCount;
        this.transactions = transactions;
        this.transactionCounts = transactionCounts;
        this.totalTransactionCount = totalTransactionCount;
        this.filteredTransactionCount = filteredTransactionCount;
    }

    public ImmutableTransactionStore(StreamInput in, BigArrays bigArrays) throws IOException {
        super(bigArrays);

        // we allocate big arrays so we have to `close` if we fail here or we'll leak them.
        boolean success = false;

        try {
            this.items = new BytesRefArray(in, bigArrays);

            long itemCountsSize = in.readVLong();
            this.itemCounts = bigArrays.newLongArray(itemCountsSize, true);
            for (int i = 0; i < itemCountsSize; ++i) {
                itemCounts.set(i, in.readVLong());
            }
            this.totalItemCount = in.readVLong();
            this.transactions = new BytesRefArray(in, bigArrays);

            long transactionsCountsSize = in.readVLong();
            this.transactionCounts = bigArrays.newLongArray(transactionsCountsSize, true);
            for (int i = 0; i < transactionsCountsSize; ++i) {
                transactionCounts.set(i, in.readVLong());
            }
            this.totalTransactionCount = in.readVLong();

            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
                this.filteredTransactionCount = in.readVLong();
            } else {
                this.filteredTransactionCount = 0;
            }

            success = true;
        } finally {
            if (false == success) {
                close();
            }
        }
    }

    @Override
    public BytesRefArray getItems() {
        return items;
    }

    @Override
    public LongArray getItemCounts() {
        return itemCounts;
    }

    @Override
    public long getTotalItemCount() {
        return totalItemCount;
    }

    @Override
    public BytesRefArray getTransactions() {
        return transactions;
    }

    @Override
    public LongArray getTransactionCounts() {
        return transactionCounts;
    }

    @Override
    public long getTotalTransactionCount() {
        return totalTransactionCount;
    }

    @Override
    public long getFilteredTransactionCount() {
        return filteredTransactionCount;
    }

    @Override
    public long ramBytesUsed() {
        return super.ramBytesUsed() + BASE_RAM_BYTES_USED + +items.ramBytesUsed() + itemCounts.ramBytesUsed() + transactions.ramBytesUsed()
            + transactionCounts.ramBytesUsed();
    }

    @Override
    public void close() {
        Releasables.close(items, itemCounts, transactions, transactionCounts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        items.writeTo(out);
        long itemCountsSize = items.size();
        long transactionCountsSize = transactions.size();
        out.writeVLong(itemCountsSize);
        for (int i = 0; i < itemCountsSize; ++i) {
            out.writeVLong(itemCounts.get(i));
        }
        out.writeVLong(totalItemCount);
        transactions.writeTo(out);
        out.writeVLong(transactionCountsSize);
        for (int i = 0; i < transactionCountsSize; ++i) {
            out.writeVLong(transactionCounts.get(i));
        }
        out.writeVLong(totalTransactionCount);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            out.writeVLong(filteredTransactionCount);
        }
    }

}
