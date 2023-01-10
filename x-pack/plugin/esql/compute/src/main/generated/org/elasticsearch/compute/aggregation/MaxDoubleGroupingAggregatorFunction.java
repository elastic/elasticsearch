package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.Optional;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link MaxDoubleAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MaxDoubleGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private final DoubleArrayState state;

  private final int channel;

  public MaxDoubleGroupingAggregatorFunction(int channel, DoubleArrayState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MaxDoubleGroupingAggregatorFunction create(BigArrays bigArrays, int channel) {
    return new MaxDoubleGroupingAggregatorFunction(channel, new DoubleArrayState(bigArrays, MaxDoubleAggregator.init()));
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
      state.set(MaxDoubleAggregator.combine(state.getOrDefault(groupId), vector.getDouble(i)), groupId);
    }
  }

  private void addRawBlock(Vector groupIdVector, Block block) {
    for (int i = 0; i < block.getTotalValueCount(); i++) {
      if (block.isNull(i) == false) {
        int groupId = Math.toIntExact(groupIdVector.getLong(i));
        state.set(MaxDoubleAggregator.combine(state.getOrDefault(groupId), block.getDouble(i)), groupId);
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
    @SuppressWarnings("unchecked") AggregatorStateVector<DoubleArrayState> blobVector = (AggregatorStateVector<DoubleArrayState>) vector.get();
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    DoubleArrayState tmpState = new DoubleArrayState(bigArrays, MaxDoubleAggregator.init());
    blobVector.get(0, tmpState);
    for (int i = 0; i < groupIdVector.getPositionCount(); i++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(i));
      state.set(MaxDoubleAggregator.combine(state.getOrDefault(groupId), tmpState.get(i)), groupId);
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    DoubleArrayState inState = ((MaxDoubleGroupingAggregatorFunction) input).state;
    state.set(MaxDoubleAggregator.combine(state.getOrDefault(groupId), inState.get(position)), groupId);
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<DoubleArrayState>, DoubleArrayState> builder =
        AggregatorStateVector.builderOfAggregatorState(DoubleArrayState.class, state.getEstimatedSize());
    builder.add(state);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    int positions = state.largestIndex + 1;
    double[] values = new double[positions];
    for (int i = 0; i < positions; i++) {
      values[i] = state.get(i);
    }
    return new DoubleVector(values, positions).asBlock();
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
