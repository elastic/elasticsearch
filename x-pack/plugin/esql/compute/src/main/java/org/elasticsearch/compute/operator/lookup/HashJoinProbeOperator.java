/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingToIteratorOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.Objects;

/**
 * Looks up probe keys after the build side is ready and appends matching build-row positions.
 */
public final class HashJoinProbeOperator extends AbstractPageMappingToIteratorOperator {
    public record Factory(HashJoinBridge bridge, int[] keyChannels, WarningSourceLocation warningSource)
        implements
            Operator.OperatorFactory {
        public Factory {
            Objects.requireNonNull(bridge);
            keyChannels = Objects.requireNonNull(keyChannels).clone();
            Objects.requireNonNull(warningSource);
            if (keyChannels.length == 0) {
                throw new IllegalArgumentException("expected at least one hash join key");
            }
        }

        @Override
        public HashJoinProbeOperator get(DriverContext driverContext) {
            Warnings warnings = Warnings.createWarnings(driverContext, warningSource);
            bridge.incRef();
            return new HashJoinProbeOperator(bridge, keyChannels, driverContext.blockFactory(), warnings);
        }

        @Override
        public String describe() {
            return "HashJoinProbe[keyChannels=" + Arrays.toString(keyChannels) + "]";
        }
    }

    private final HashJoinBridge bridge;
    private final int[] keyChannels;
    private final BlockFactory blockFactory;
    private final Warnings warnings;
    private final Releasable releaseBridge;

    private HashJoinProbeOperator(HashJoinBridge bridge, int[] keyChannels, BlockFactory blockFactory, Warnings warnings) {
        this.bridge = bridge;
        this.keyChannels = keyChannels;
        this.blockFactory = blockFactory;
        this.warnings = warnings;
        this.releaseBridge = Releasables.releaseOnce(bridge::decRef);
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        return new ReleasableIterator<>() {
            private Page pending = page;
            private ReleasableIterator<Page> delegate;

            @Override
            public boolean hasNext() {
                return delegate == null ? pending != null : delegate.hasNext();
            }

            @Override
            public Page next() {
                if (delegate == null) {
                    delegate = lookup(pending);
                    pending = null;
                }
                return delegate.next();
            }

            @Override
            public void close() {
                Releasables.close(delegate, pending == null ? null : pending::releaseBlocks);
            }
        };
    }

    private ReleasableIterator<Page> lookup(Page page) {
        Page mapped = page.projectBlocks(keyChannels);
        try {
            RowInTableLookup lookup = bridge.lookup();
            return appendBlocks(page, lookup.lookup(mapped, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE, blockFactory, warnings));
        } finally {
            mapped.releaseBlocks();
        }
    }

    @Override
    public IsBlockedResult isBlocked() {
        SubscribableListener<Void> ready = bridge.ready();
        return ready.isDone() ? Operator.NOT_BLOCKED : new IsBlockedResult(ready, "hash join build");
    }

    @Override
    public void close() {
        Releasables.close(super::close, releaseBridge);
    }

    @Override
    public String toString() {
        return "HashJoinProbe[keyChannels=" + Arrays.toString(keyChannels) + "]";
    }
}
