package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.Optional;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link AggregatorFunction} implementation for {@link MaxLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MaxLongAggregatorFunction implements AggregatorFunction {
  private final LongState state;

  private final int channel;

  public MaxLongAggregatorFunction(int channel, LongState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MaxLongAggregatorFunction create(int channel) {
    return new MaxLongAggregatorFunction(channel, new LongState(MaxLongAggregator.init()));
  }

  @Override
  public void addRawInput(Page page) {
    assert channel >= 0;
    Block block = page.getBlock(channel);
    Optional<Vector> vector = block.asVector();
    if (vector.isPresent()) {
      addRawVector(vector.get());
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(Vector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      state.longValue(MaxLongAggregator.combine(state.longValue(), vector.getLong(i)));
    }
  }

  private void addRawBlock(Block block) {
    for (int i = 0; i < block.getTotalValueCount(); i++) {
      if (block.isNull(i) == false) {
        state.longValue(MaxLongAggregator.combine(state.longValue(), block.getLong(i)));
      }
    }
  }

  @Override
  public void addIntermediateInput(Block block) {
    assert channel == -1;
    Optional<Vector> vector = block.asVector();
    if (vector.isEmpty() || vector.get() instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<LongState> blobVector = (AggregatorStateVector<LongState>) vector.get();
    LongState tmpState = new LongState();
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      state.longValue(MaxLongAggregator.combine(state.longValue(), tmpState.longValue()));
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
    return new LongVector(new long[] { state.longValue() }, 1).asBlock();
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
