package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.Optional;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link AggregatorFunction} implementation for {@link MinDoubleAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MinDoubleAggregatorFunction implements AggregatorFunction {
  private final DoubleState state;

  private final int channel;

  public MinDoubleAggregatorFunction(int channel, DoubleState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MinDoubleAggregatorFunction create(int channel) {
    return new MinDoubleAggregatorFunction(channel, new DoubleState(MinDoubleAggregator.init()));
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
      state.doubleValue(MinDoubleAggregator.combine(state.doubleValue(), vector.getDouble(i)));
    }
  }

  private void addRawBlock(Block block) {
    for (int i = 0; i < block.getTotalValueCount(); i++) {
      if (block.isNull(i) == false) {
        state.doubleValue(MinDoubleAggregator.combine(state.doubleValue(), block.getDouble(i)));
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
    @SuppressWarnings("unchecked") AggregatorStateVector<DoubleState> blobVector = (AggregatorStateVector<DoubleState>) vector.get();
    DoubleState tmpState = new DoubleState();
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      state.doubleValue(MinDoubleAggregator.combine(state.doubleValue(), tmpState.doubleValue()));
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
    return new DoubleVector(new double[] { state.doubleValue() }, 1).asBlock();
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
