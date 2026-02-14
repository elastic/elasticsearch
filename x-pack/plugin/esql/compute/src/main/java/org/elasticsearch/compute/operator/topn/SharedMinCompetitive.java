/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.SideChannel;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * A thread safe, shared holder for the min competitive value from a
 * set of {@link TopNOperator}s.
 */
public class SharedMinCompetitive extends AbstractRefCounted implements Releasable, RefCounted, SideChannel {
    public static class Supplier {
        private final CircuitBreaker breaker;
        private final List<ElementType> elementTypes;
        private final List<TopNEncoder> encoders;
        private final List<TopNOperator.SortOrder> sortOrders;
        private SharedMinCompetitive minCompetitive;

        public Supplier(
            CircuitBreaker breaker,
            List<ElementType> elementTypes,
            List<TopNEncoder> encoders,
            List<TopNOperator.SortOrder> sortOrders
        ) {
            this.breaker = breaker;
            this.elementTypes = elementTypes;
            this.encoders = encoders;
            this.sortOrders = sortOrders;
        }

        public SharedMinCompetitive get() {
            if (minCompetitive == null) {
                minCompetitive = new SharedMinCompetitive(breaker, elementTypes, encoders, sortOrders);
            } else {
                minCompetitive.incRef();
            }
            return minCompetitive;
        }
    }

    private final BreakingBytesRefBuilder value;
    private final List<ElementType> elementTypes;
    private final List<TopNEncoder> encoders;
    private final List<TopNOperator.SortOrder> sortOrders;

    SharedMinCompetitive(
        CircuitBreaker breaker,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<TopNOperator.SortOrder> sortOrders
    ) {
        this.value = new BreakingBytesRefBuilder(breaker, "min_competitive");
        this.elementTypes = elementTypes;
        this.encoders = encoders;
        this.sortOrders = sortOrders;
    }

    /**
     * Offer an update to the min competitive value.
     * @param minCompetitive if it is accepted then the bytes are copied
     */
    public void offer(BytesRef minCompetitive) {
        if (value.bytesRefView().compareTo(minCompetitive) >= 0) {
            return;
        }
        synchronized (value) {
            if (value.bytesRefView().compareTo(minCompetitive) >= 0) {
                return;
            }
            value.clear();
            value.append(minCompetitive);
        }
    }

    /**
     * Read the min competitive value. This will return {@code null} if there
     * isn't yet a min competitive value. Otherwise, this will return a
     * {@link Page} that contains single-position, single-valued {@link Block}s.
     */
    @Nullable
    public Page get(BlockFactory blockFactory) {
        try (BreakingBytesRefBuilder copy = new BreakingBytesRefBuilder(blockFactory.breaker(), "min_competitive_copy")) {
            synchronized (value) {
                if (value.bytesRefView().length == 0) {
                    // Not assigned anything yet
                    return null;
                }
                copy.append(value.bytesRefView());
            }
            ResultBuilder[] builders = new ResultBuilder[sortOrders.size()];
            BytesRef shallow = copy.bytesRefView();
            try {
                for (int i = 0; i < builders.length; i++) {
                    TopNOperator.SortOrder sortOrder = sortOrders.get(i);
                    TopNEncoder encoder = encoders.get(i);
                    ElementType elementType = elementTypes.get(i);
                    ResultBuilder builder = ResultBuilder.resultBuilderFor(blockFactory, elementType, encoder, true, 1);
                    builders[i] = builder;
                    if (shallow.bytes[shallow.offset] == sortOrder.nul()) {
                        builder.decodeValue(new BytesRef(new byte[] { 0 }));
                        shallow.offset += 1;
                        shallow.length -= 1;
                        continue;
                    }
                    shallow.offset += 1;
                    shallow.length -= 1;
                    builder.decodeKey(shallow, sortOrder.asc());
                    builder.decodeValue(new BytesRef(new byte[] { 1 }));
                }
                return new Page(ResultBuilder.buildAll(builders));
            } finally {
                Releasables.close(builders);
            }
        }
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        value.close();
    }
}
