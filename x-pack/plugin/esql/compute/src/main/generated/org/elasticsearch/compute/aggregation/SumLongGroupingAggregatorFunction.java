package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SumLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SumLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private final LongArrayState state;

  private final int channel;

  public SumLongGroupingAggregatorFunction(int channel, LongArrayState state) {
    this.channel = channel;
    this.state = state;
  }

  public static SumLongGroupingAggregatorFunction create(BigArrays bigArrays, int channel) {
    return new SumLongGroupingAggregatorFunction(channel, new LongArrayState(bigArrays, SumLongAggregator.init()));
  }

  @Override
  public void addRawInput(LongVector groupIdVector, Page page) {
    assert channel >= 0;
    LongBlock block = page.getBlock(channel);
    LongVector vector = block.asVector();
    if (vector != null) {
      addRawVector(groupIdVector, vector);
    } else {
      addRawBlock(groupIdVector, block);
    }
  }

  private void addRawVector(LongVector groupIdVector, LongVector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(i));
      state.set(SumLongAggregator.combine(state.getOrDefault(groupId), vector.getLong(i)), groupId);
    }
  }

  private void addRawBlock(LongVector groupIdVector, LongBlock block) {
    for (int i = 0; i < block.getTotalValueCount(); i++) {
      if (block.isNull(i) == false) {
        int groupId = Math.toIntExact(groupIdVector.getLong(i));
        state.set(SumLongAggregator.combine(state.getOrDefault(groupId), block.getLong(i)), groupId);
      }
    }
  }

  @Override
  public void addIntermediateInput(LongVector groupIdVector, Block block) {
    assert channel == -1;
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<LongArrayState> blobVector = (AggregatorStateVector<LongArrayState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    LongArrayState inState = new LongArrayState(bigArrays, SumLongAggregator.init());
    blobVector.get(0, inState);
    for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(position));
      state.set(SumLongAggregator.combine(state.getOrDefault(groupId), inState.get(position)), groupId);
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    LongArrayState inState = ((SumLongGroupingAggregatorFunction) input).state;
    state.set(SumLongAggregator.combine(state.getOrDefault(groupId), inState.get(position)), groupId);
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<LongArrayState>, LongArrayState> builder =
        AggregatorStateVector.builderOfAggregatorState(LongArrayState.class, state.getEstimatedSize());
    builder.add(state);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    int positions = state.largestIndex + 1;
    long[] values = new long[positions];
    for (int i = 0; i < positions; i++) {
      values[i] = state.get(i);
    }
    return new LongArrayVector(values, positions).asBlock();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("channel=").append(channel);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    state.close();
  }
}
