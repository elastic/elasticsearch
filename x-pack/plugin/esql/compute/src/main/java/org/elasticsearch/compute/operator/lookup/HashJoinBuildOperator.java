/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Materializes a join's build side once and publishes its keys and payload columns.
 */
public final class HashJoinBuildOperator extends SinkOperator {
    /**
     * Retains a stage-owned bridge when {@code retainBridge} is true; otherwise the build operator takes its initial reference.
     */
    public record Factory(
        HashJoinBridge bridge,
        int[] keyChannels,
        ElementType[] keyTypes,
        int[] payloadChannels,
        ElementType[] payloadTypes,
        WarningSourceLocation warningSource,
        boolean retainBridge
    ) implements SinkOperatorFactory {
        public Factory(
            HashJoinBridge bridge,
            int[] keyChannels,
            ElementType[] keyTypes,
            int[] payloadChannels,
            ElementType[] payloadTypes,
            WarningSourceLocation warningSource
        ) {
            this(bridge, keyChannels, keyTypes, payloadChannels, payloadTypes, warningSource, false);
        }

        public Factory {
            Objects.requireNonNull(bridge);
            Objects.requireNonNull(warningSource);
            keyChannels = Objects.requireNonNull(keyChannels).clone();
            keyTypes = Objects.requireNonNull(keyTypes).clone();
            payloadChannels = Objects.requireNonNull(payloadChannels).clone();
            payloadTypes = Objects.requireNonNull(payloadTypes).clone();
            if (keyChannels.length == 0) {
                throw new IllegalArgumentException("expected at least one hash join key");
            }
            if (keyChannels.length != keyTypes.length || payloadChannels.length != payloadTypes.length) {
                throw new IllegalArgumentException("channel and element type counts must match");
            }
        }

        @Override
        public HashJoinBuildOperator get(DriverContext driverContext) {
            HashJoinBuildOperator operator = new HashJoinBuildOperator(this, driverContext);
            if (retainBridge) {
                bridge.incRef();
            }
            return operator;
        }

        @Override
        public String describe() {
            return "HashJoinBuild[keyChannels="
                + Arrays.toString(keyChannels)
                + ", payloadChannels="
                + Arrays.toString(payloadChannels)
                + "]";
        }
    }

    private final HashJoinBridge bridge;
    private final int[] inputChannels;
    private final ElementType[] inputTypes;
    private final int keyCount;
    private final DriverContext driverContext;
    private final Warnings warnings;
    private final List<Page> pages = new ArrayList<>();

    private boolean finished;
    private boolean published;
    private boolean closed;

    private HashJoinBuildOperator(Factory factory, DriverContext driverContext) {
        this.bridge = factory.bridge();
        this.keyCount = factory.keyChannels().length;
        this.driverContext = driverContext;
        this.warnings = Warnings.createWarnings(driverContext, factory.warningSource());
        this.inputChannels = new int[factory.keyChannels().length + factory.payloadChannels().length];
        this.inputTypes = new ElementType[factory.keyTypes().length + factory.payloadTypes().length];
        System.arraycopy(factory.keyChannels(), 0, inputChannels, 0, factory.keyChannels().length);
        System.arraycopy(factory.payloadChannels(), 0, inputChannels, keyCount, factory.payloadChannels().length);
        System.arraycopy(factory.keyTypes(), 0, inputTypes, 0, factory.keyTypes().length);
        System.arraycopy(factory.payloadTypes(), 0, inputTypes, keyCount, factory.payloadTypes().length);
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    protected void doAddInput(Page page) {
        Page projected = null;
        try {
            for (int i = 0; i < inputChannels.length; i++) {
                ElementType actualType = page.getBlock(inputChannels[i]).elementType();
                if (actualType != inputTypes[i]) {
                    throw new IllegalArgumentException(
                        "expected build column [" + inputChannels[i] + "] to have type [" + inputTypes[i] + "] but got [" + actualType + "]"
                    );
                }
            }
            projected = page.projectBlocks(inputChannels);
            pages.add(projected);
            projected = null;
        } catch (RuntimeException e) {
            bridge.fail(e);
            throw e;
        } finally {
            if (projected != null) {
                projected.releaseBlocks();
            }
            page.releaseBlocks();
        }
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        buildLookup();
    }

    private void buildLookup() {
        Block.Builder[] builders = new Block.Builder[inputTypes.length];
        Block[] keyBlocks = new Block[keyCount];
        Block[] payloadBlocks = new Block[inputTypes.length - keyCount];
        RowInTableLookup lookup = null;
        try {
            int totalPositions = 0;
            for (Page page : pages) {
                totalPositions = Math.addExact(totalPositions, page.getPositionCount());
            }
            BlockFactory parent = driverContext.blockFactory().parent();
            for (int column = 0; column < inputTypes.length; column++) {
                if (inputTypes[column] != ElementType.NULL) {
                    builders[column] = inputTypes[column].newBlockBuilder(totalPositions, parent);
                }
            }

            int retainedPositions = 0;
            boolean sawMultiValue = false;
            for (Page page : pages) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    boolean keep = true;
                    for (int key = 0; key < keyCount; key++) {
                        int valueCount = page.getBlock(key).getValueCount(position);
                        if (valueCount > 1) {
                            sawMultiValue = true;
                        }
                        if (valueCount != 1) {
                            keep = false;
                        }
                    }
                    if (keep) {
                        for (int column = 0; column < builders.length; column++) {
                            if (builders[column] != null) {
                                builders[column].copyFrom(page.getBlock(column), position, position + 1);
                            }
                        }
                        retainedPositions++;
                    }
                }
            }

            for (int column = 0; column < inputTypes.length; column++) {
                Block block;
                if (builders[column] == null) {
                    block = parent.newConstantNullBlock(retainedPositions);
                } else {
                    Block.Builder builder = builders[column];
                    builders[column] = null;
                    try (builder) {
                        block = builder.build();
                    }
                }
                if (column < keyCount) {
                    keyBlocks[column] = block;
                } else {
                    payloadBlocks[column - keyCount] = block;
                }
            }

            if (sawMultiValue) {
                warnings.registerException(new IllegalArgumentException("LOOKUP JOIN encountered multi-value"));
            }
            lookup = RowInTableLookup.build(parent, keyBlocks, true, Warnings.NOOP_WARNINGS);
            Releasables.closeExpectNoException(keyBlocks);
            keyBlocks = null;
            bridge.publish(lookup, payloadBlocks);
            lookup = null;
            payloadBlocks = null;
            published = true;
        } catch (RuntimeException e) {
            bridge.fail(e);
            throw e;
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(builders));
            if (keyBlocks != null) {
                Releasables.closeExpectNoException(keyBlocks);
            }
            if (payloadBlocks != null) {
                Releasables.closeExpectNoException(payloadBlocks);
            }
            Releasables.closeExpectNoException(lookup);
        }
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (published == false) {
            bridge.fail(new IllegalStateException("hash join build side did not complete"));
        }
        Releasables.closeExpectNoException(
            Releasables.wrap(pages.stream().<Releasable>map(page -> page::releaseBlocks).toList()),
            bridge::decRef
        );
        pages.clear();
    }

    @Override
    public String toString() {
        return "HashJoinBuild[keyCount=" + keyCount + "]";
    }
}
