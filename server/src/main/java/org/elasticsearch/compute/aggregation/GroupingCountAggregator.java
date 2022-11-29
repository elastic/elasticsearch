/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.AggregatorStateBlock;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.Page;

@Experimental
public class GroupingCountAggregator implements GroupingAggregatorFunction {

    private final LongArrayState state;
    private final int channel;

    static GroupingCountAggregator create(int inputChannel) {
        if (inputChannel < 0) {
            throw new IllegalArgumentException();
        }
        return new GroupingCountAggregator(inputChannel, new LongArrayState(0));
    }

    static GroupingCountAggregator createIntermediate() {
        return new GroupingCountAggregator(-1, new LongArrayState(0));
    }

    private GroupingCountAggregator(int channel, LongArrayState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Block groupIdBlock, Page page) {
        assert channel >= 0;
        Block valuesBlock = page.getBlock(channel);
        LongArrayState s = this.state;
        int len = valuesBlock.getPositionCount();
        for (int i = 0; i < len; i++) {
            if (groupIdBlock.isNull(i) == false) {
                int groupId = (int) groupIdBlock.getLong(i);
                s.increment(1, groupId);
            }
        }
    }

    @Override
    public void addIntermediateInput(Block groupIdBlock, Block block) {
        assert channel == -1;
        if (block instanceof AggregatorStateBlock) {
            @SuppressWarnings("unchecked")
            AggregatorStateBlock<LongArrayState> blobBlock = (AggregatorStateBlock<LongArrayState>) block;
            LongArrayState tmpState = new LongArrayState(0);
            blobBlock.get(0, tmpState);
            final int positions = groupIdBlock.getPositionCount();
            final LongArrayState s = state;
            for (int i = 0; i < positions; i++) {
                if (groupIdBlock.isNull(i) == false) {
                    int groupId = (int) groupIdBlock.getLong(i);
                    s.increment(tmpState.get(i), groupId);
                }
            }
        } else {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
    }

    @Override
    public Block evaluateIntermediate() {
        AggregatorStateBlock.Builder<AggregatorStateBlock<LongArrayState>, LongArrayState> builder = AggregatorStateBlock
            .builderOfAggregatorState(LongArrayState.class, state.getEstimatedSize());
        builder.add(state);
        return builder.build();
    }

    @Override
    public Block evaluateFinal() {
        LongArrayState s = state;
        int positions = s.largestIndex + 1;
        long[] result = new long[positions];
        for (int i = 0; i < positions; i++) {
            result[i] = s.get(i);
        }
        return new LongArrayBlock(result, positions);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channel=").append(channel);
        sb.append("]");
        return sb.toString();
    }
}
