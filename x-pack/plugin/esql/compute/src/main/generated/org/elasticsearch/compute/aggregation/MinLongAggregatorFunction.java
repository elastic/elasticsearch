package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link AggregatorFunction} implementation for {@link MinLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MinLongAggregatorFunction implements AggregatorFunction {
  private final LongState state;

  private final int channel;

  public MinLongAggregatorFunction(int channel, LongState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MinLongAggregatorFunction create(int channel) {
    return new MinLongAggregatorFunction(channel, new LongState(MinLongAggregator.init()));
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
      state.longValue(MinLongAggregator.combine(state.longValue(), vector.getLong(i)));
    }
  }

  private void addRawBlock(LongBlock block) {
    for (int i = 0; i < block.getTotalValueCount(); i++) {
      if (block.isNull(i) == false) {
        state.longValue(MinLongAggregator.combine(state.longValue(), block.getLong(i)));
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
    @SuppressWarnings("unchecked") AggregatorStateVector<LongState> blobVector = (AggregatorStateVector<LongState>) vector;
    LongState tmpState = new LongState();
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      state.longValue(MinLongAggregator.combine(state.longValue(), tmpState.longValue()));
    }
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<LongState>, LongState> builder =
        AggregatorStateVector.builderOfAggregatorState(LongState.class, state.getEstimatedSize());
    builder.add(state);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    return new LongArrayVector(new long[] { state.longValue() }, 1).asBlock();
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
