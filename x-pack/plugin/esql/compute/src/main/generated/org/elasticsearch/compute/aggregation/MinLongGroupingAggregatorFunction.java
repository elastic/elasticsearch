package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.Optional;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link MinLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MinLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private final LongArrayState state;

  private final int channel;

  public MinLongGroupingAggregatorFunction(int channel, LongArrayState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MinLongGroupingAggregatorFunction create(BigArrays bigArrays, int channel) {
    return new MinLongGroupingAggregatorFunction(channel, new LongArrayState(bigArrays, MinLongAggregator.init()));
  }

  @Override
  public void addRawInput(Vector groupIdVector, Page page) {
    assert channel >= 0;
    Block block = page.getBlock(channel);
    Optional<Vector> vector = block.asVector();
    if (vector.isPresent()) {
      addRawVector(groupIdVector, vector.get());
    } else {
      addRawBlock(groupIdVector, block);
    }
  }

  private void addRawVector(Vector groupIdVector, Vector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(i));
      state.set(MinLongAggregator.combine(state.getOrDefault(groupId), vector.getLong(i)), groupId);
    }
  }

  private void addRawBlock(Vector groupIdVector, Block block) {
    for (int i = 0; i < block.getTotalValueCount(); i++) {
      if (block.isNull(i) == false) {
        int groupId = Math.toIntExact(groupIdVector.getLong(i));
        state.set(MinLongAggregator.combine(state.getOrDefault(groupId), block.getLong(i)), groupId);
      }
    }
  }

  @Override
  public void addIntermediateInput(Vector groupIdVector, Block block) {
    assert channel == -1;
    Optional<Vector> vector = block.asVector();
    if (vector.isEmpty() || vector.get() instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<LongArrayState> blobVector = (AggregatorStateVector<LongArrayState>) vector.get();
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    LongArrayState tmpState = new LongArrayState(bigArrays, MinLongAggregator.init());
    blobVector.get(0, tmpState);
    for (int i = 0; i < groupIdVector.getPositionCount(); i++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(i));
      state.set(MinLongAggregator.combine(state.getOrDefault(groupId), tmpState.get(i)), groupId);
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    LongArrayState inState = ((MinLongGroupingAggregatorFunction) input).state;
    state.set(MinLongAggregator.combine(state.getOrDefault(groupId), inState.get(position)), groupId);
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
    return new LongVector(values, positions).asBlock();
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
