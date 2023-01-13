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
 * {@link AggregatorFunction} implementation for {@link AvgLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class AvgLongAggregatorFunction implements AggregatorFunction {
  private final AvgLongAggregator.AvgState state;

  private final int channel;

  public AvgLongAggregatorFunction(int channel, AvgLongAggregator.AvgState state) {
    this.channel = channel;
    this.state = state;
  }

  public static AvgLongAggregatorFunction create(int channel) {
    return new AvgLongAggregatorFunction(channel, AvgLongAggregator.initSingle());
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
      AvgLongAggregator.combine(state, vector.getLong(i));
    }
    AvgLongAggregator.combineValueCount(state, vector.getPositionCount());
  }

  private void addRawBlock(LongBlock block) {
    for (int i = 0; i < block.getTotalValueCount(); i++) {
      if (block.isNull(i) == false) {
        AvgLongAggregator.combine(state, block.getLong(i));
      }
    }
    AvgLongAggregator.combineValueCount(state, block.validPositionCount());
  }

  @Override
  public void addIntermediateInput(Block block) {
    assert channel == -1;
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<AvgLongAggregator.AvgState> blobVector = (AggregatorStateVector<AvgLongAggregator.AvgState>) vector;
    AvgLongAggregator.AvgState tmpState = new AvgLongAggregator.AvgState();
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      AvgLongAggregator.combineStates(state, tmpState);
    }
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<AvgLongAggregator.AvgState>, AvgLongAggregator.AvgState> builder =
        AggregatorStateVector.builderOfAggregatorState(AvgLongAggregator.AvgState.class, state.getEstimatedSize());
    builder.add(state);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    return AvgLongAggregator.evaluateFinal(state);
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
