package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link AggregatorFunction} implementation for {@link MedianAbsoluteDeviationLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MedianAbsoluteDeviationLongAggregatorFunction implements AggregatorFunction {
  private final QuantileStates.SingleState state;

  private final int channel;

  public MedianAbsoluteDeviationLongAggregatorFunction(int channel,
      QuantileStates.SingleState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MedianAbsoluteDeviationLongAggregatorFunction create(int channel) {
    return new MedianAbsoluteDeviationLongAggregatorFunction(channel, MedianAbsoluteDeviationLongAggregator.initSingle());
  }

  @Override
  public void addRawInput(Page page) {
    assert channel >= 0;
    ElementType type = page.getBlock(channel).elementType();
    if (type == ElementType.NULL) {
      return;
    }
    LongBlock block;
    if (type == ElementType.INT) {
      block = page.<IntBlock>getBlock(channel).asLongBlock();
    } else {
      block = page.getBlock(channel);
    }
    LongVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector);
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(LongVector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      MedianAbsoluteDeviationLongAggregator.combine(state, vector.getLong(i));
    }
  }

  private void addRawBlock(LongBlock block) {
    for (int i = 0; i < block.getTotalValueCount(); i++) {
      if (block.isNull(i) == false) {
        MedianAbsoluteDeviationLongAggregator.combine(state, block.getLong(i));
      }
    }
  }

  @Override
  public void addIntermediateInput(Block block) {
    assert channel == -1;
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<QuantileStates.SingleState> blobVector = (AggregatorStateVector<QuantileStates.SingleState>) vector;
    QuantileStates.SingleState tmpState = new QuantileStates.SingleState();
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      MedianAbsoluteDeviationLongAggregator.combineStates(state, tmpState);
    }
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<QuantileStates.SingleState>, QuantileStates.SingleState> builder =
        AggregatorStateVector.builderOfAggregatorState(QuantileStates.SingleState.class, state.getEstimatedSize());
    builder.add(state);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    return MedianAbsoluteDeviationLongAggregator.evaluateFinal(state);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("channel=").append(channel);
    sb.append("]");
    return sb.toString();
  }
}
