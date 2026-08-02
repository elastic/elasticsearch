/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Pass-through operator that fails the query when two distinct rows share the same composite key. It is inserted at the
 * PromQL relabel seam (the first-pass identity aggregation, before any consuming outer aggregate or the final
 * time-series collapse) so it observes one row per source series per time bucket. The key channels are the rewritten
 * series identity plus the time bucket, so a repeated key means {@code label_replace}/{@code label_join} mapped two
 * different source series onto the same label set at the same instant.
 * <p>
 * This mirrors PromQL's "vector cannot contain metrics with the same labelset" evaluation error and, like Prometheus,
 * surfaces it as a hard failure: the first collision throws an {@link IllegalArgumentException}, which propagates to the
 * client as a {@code 400 Bad Request}. Rows that do not collide are passed through unchanged. Series whose identities
 * collide only at different buckets do not repeat a key and so pass silently, matching Prometheus.
 */
public class PromqlCollisionCheckOperator extends AbstractPageMappingOperator {

    /** Matches Prometheus' evaluation error text so the failure is recognizable to PromQL users. */
    static final String COLLISION_ERROR = "vector cannot contain metrics with the same labelset";

    public record Factory(List<Integer> keyChannels) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            int[] channels = keyChannels.stream().mapToInt(Integer::intValue).toArray();
            return new PromqlCollisionCheckOperator(driverContext, channels);
        }

        @Override
        public String describe() {
            return "PromqlCollisionCheckOperator[keyChannels=" + keyChannels + "]";
        }
    }

    private final DriverContext driverContext;
    private final int[] keyChannels;
    private final BytesRefHashTable seenKeys;

    private GroupKeyEncoder encoder;

    public PromqlCollisionCheckOperator(DriverContext driverContext, int[] keyChannels) {
        this.driverContext = driverContext;
        this.keyChannels = keyChannels;
        this.seenKeys = HashImplFactory.newBytesRefHash(driverContext.blockFactory());
    }

    @Override
    protected Page process(Page page) {
        if (encoder == null) {
            initEncoder(page);
        }
        int positionCount = page.getPositionCount();
        for (int p = 0; p < positionCount; p++) {
            PagedBytesCursor key = encoder.encode(page, p);
            // A negative id means the (identity, bucket) key was already present: a second source series maps here.
            if (seenKeys.add(key) < 0) {
                throw new IllegalArgumentException(COLLISION_ERROR);
            }
        }
        return page;
    }

    private void initEncoder(Page page) {
        List<ElementType> elementTypes = new ArrayList<>(page.getBlockCount());
        for (int i = 0; i < page.getBlockCount(); i++) {
            elementTypes.add(page.getBlock(i).elementType());
        }
        BlockFactory blockFactory = driverContext.blockFactory();
        PagedBytesBuilder row = new PagedBytesBuilder(
            blockFactory.bigArrays().recycler(),
            blockFactory.breaker(),
            "promql-collision-key-encoder",
            64
        );
        encoder = new GroupKeyEncoder(keyChannels, elementTypes, row);
    }

    @Override
    public String toString() {
        return "PromqlCollisionCheckOperator[keyChannels=" + Arrays.toString(keyChannels) + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(encoder, seenKeys);
        super.close();
    }
}
