package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link AggregatorFunction} implementation for {@link MaxDoubleAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MaxDoubleAggregatorFunction implements AggregatorFunction {
  private final DoubleState state;

  private final int channel;

  public MaxDoubleAggregatorFunction(int channel, DoubleState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MaxDoubleAggregatorFunction create(int channel) {
    return new MaxDoubleAggregatorFunction(channel, new DoubleState(MaxDoubleAggregator.init()));
  }

  @Override
  public void addRawInput(Page page) {
    assert channel >= 0;
    ElementType type = page.getBlock(channel).elementType();
    if (type == ElementType.NULL) {
      return;
    }
    DoubleBlock block = page.getBlock(channel);
    DoubleVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector);
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(DoubleVector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), vector.getDouble(i)));
    }
  }

  private void addRawBlock(DoubleBlock block) {
    for (int i = 0; i < block.getTotalValueCount(); i++) {
      if (block.isNull(i) == false) {
        state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), block.getDouble(i)));
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
    @SuppressWarnings("unchecked") AggregatorStateVector<DoubleState> blobVector = (AggregatorStateVector<DoubleState>) vector;
    DoubleState tmpState = new DoubleState();
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), tmpState.doubleValue()));
    }
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<DoubleState>, DoubleState> builder =
        AggregatorStateVector.builderOfAggregatorState(DoubleState.class, state.getEstimatedSize());
    builder.add(state);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    return new DoubleArrayVector(new double[] { state.doubleValue() }, 1).asBlock();
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
