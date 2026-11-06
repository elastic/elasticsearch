/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingToIteratorOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Objects;

/**
 * Loads build-side values for the matching positions emitted by a hash-join probe.
 */
public final class HashJoinColumnLoadOperator extends AbstractPageMappingToIteratorOperator {
    public record Factory(HashJoinBridge bridge, int payloadIndex, int positionsChannel) implements Operator.OperatorFactory {
        public Factory {
            Objects.requireNonNull(bridge);
            if (payloadIndex < 0 || positionsChannel < 0) {
                throw new IllegalArgumentException("payload index and positions channel must be non-negative");
            }
        }

        @Override
        public HashJoinColumnLoadOperator get(DriverContext driverContext) {
            bridge.incRef();
            return new HashJoinColumnLoadOperator(bridge, payloadIndex, positionsChannel);
        }

        @Override
        public String describe() {
            return "HashJoinColumnLoad[payloadIndex=" + payloadIndex + ", positionsChannel=" + positionsChannel + "]";
        }
    }

    private static final ByteSizeValue TARGET_BLOCK_SIZE = ByteSizeValue.ofKb(10);

    private final HashJoinBridge bridge;
    private final int payloadIndex;
    private final int positionsChannel;
    private final Releasable releaseBridge;

    private HashJoinColumnLoadOperator(HashJoinBridge bridge, int payloadIndex, int positionsChannel) {
        this.bridge = bridge;
        this.payloadIndex = payloadIndex;
        this.positionsChannel = positionsChannel;
        this.releaseBridge = Releasables.releaseOnce(bridge::decRef);
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        return appendBlocks(page, bridge.payload(payloadIndex).lookup(page.getBlock(positionsChannel), TARGET_BLOCK_SIZE));
    }

    @Override
    public void close() {
        Releasables.close(super::close, releaseBridge);
    }

    @Override
    public String toString() {
        return "HashJoinColumnLoad[payloadIndex=" + payloadIndex + ", positionsChannel=" + positionsChannel + "]";
    }
}
