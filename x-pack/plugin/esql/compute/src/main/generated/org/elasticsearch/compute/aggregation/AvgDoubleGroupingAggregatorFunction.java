package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.Optional;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link AvgDoubleAggregator}.
 * This class is generated. Do not edit it.
 */
public final class AvgDoubleGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private final AvgDoubleAggregator.GroupingAvgState state;

  private final int channel;

  public AvgDoubleGroupingAggregatorFunction(int channel,
      AvgDoubleAggregator.GroupingAvgState state) {
    this.channel = channel;
    this.state = state;
  }

  public static AvgDoubleGroupingAggregatorFunction create(BigArrays bigArrays, int channel) {
    return new AvgDoubleGroupingAggregatorFunction(channel, AvgDoubleAggregator.initGrouping(bigArrays));
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
      AvgDoubleAggregator.combine(state, groupId, vector.getDouble(i));
    }
  }

  private void addRawBlock(Vector groupIdVector, Block block) {
    for (int i = 0; i < block.getTotalValueCount(); i++) {
      if (block.isNull(i) == false) {
        int groupId = Math.toIntExact(groupIdVector.getLong(i));
        AvgDoubleAggregator.combine(state, groupId, block.getDouble(i));
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
    @SuppressWarnings("unchecked") AggregatorStateVector<AvgDoubleAggregator.GroupingAvgState> blobVector = (AggregatorStateVector<AvgDoubleAggregator.GroupingAvgState>) vector.get();
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    AvgDoubleAggregator.GroupingAvgState inState = AvgDoubleAggregator.initGrouping(bigArrays);
    blobVector.get(0, inState);
    for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(position));
      AvgDoubleAggregator.combineStates(state, groupId, inState, position);
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    AvgDoubleAggregator.GroupingAvgState inState = ((AvgDoubleGroupingAggregatorFunction) input).state;
    AvgDoubleAggregator.combineStates(state, groupId, inState, position);
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<AvgDoubleAggregator.GroupingAvgState>, AvgDoubleAggregator.GroupingAvgState> builder =
        AggregatorStateVector.builderOfAggregatorState(AvgDoubleAggregator.GroupingAvgState.class, state.getEstimatedSize());
    builder.add(state);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    return AvgDoubleAggregator.evaluateFinal(state);
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
