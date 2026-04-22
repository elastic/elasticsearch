// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation.spatial;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * {@link AggregatorFunction} implementation for {@link SpatialExtentCartesianShapeDocValuesAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class SpatialExtentCartesianShapeDocValuesAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("minX", ElementType.INT),
      new IntermediateStateDesc("maxX", ElementType.INT),
      new IntermediateStateDesc("maxY", ElementType.INT),
      new IntermediateStateDesc("minY", ElementType.INT)  );

  private final DriverContext driverContext;

  private final SpatialExtentState state;

  private final List<ExpressionEvaluator> inputs;

  SpatialExtentCartesianShapeDocValuesAggregatorFunction(DriverContext driverContext,
      List<ExpressionEvaluator> inputs) {
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = SpatialExtentCartesianShapeDocValuesAggregator.initSingle();
    boolean success = false;
    try {
      driverContext.breaker().addEstimateBytesAndMaybeBreak(ExpressionEvaluator.totalRamBytesUsed(inputs), "ESQL");
      success = true;
    } finally {
      if (success == false) {
        this.state.close();
      }
    }
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public void addRawInput(Page page, BooleanVector mask) {
    if (mask.allFalse()) {
      // Entire page masked away
    } else if (mask.allTrue()) {
      addRawInputNotMasked(page);
    } else {
      addRawInputMasked(page, mask);
    }
  }

  private void addRawInputMasked(Page page, BooleanVector mask) {
    try (Block valuesUncast = inputs.get(0).eval(page)) {
      IntBlock valuesBlock = (IntBlock) valuesUncast;
      if (valuesBlock.areAllValuesNull()) {
        /*
         * All values are null so we can skip processing this block.
         * NOTE: Microbenchmarks point to long sequences of ConstantNullBlocks
         *       being fast without this. Likely the branch predictor is kicking
         *       in there. But we do this anyway, just so we don't have to trust
         *       it. It's magic. Glorious magic. But it's deep magic. And we won't
         *       always have long sequences of ConstantNullBlock. And this code
         *       shows readers we've thought about this.
         */
        return;
      }
      addRawBlock(valuesBlock, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (Block valuesUncast = inputs.get(0).eval(page)) {
      IntBlock valuesBlock = (IntBlock) valuesUncast;
      if (valuesBlock.areAllValuesNull()) {
        /*
         * All values are null so we can skip processing this block.
         * NOTE: Microbenchmarks point to long sequences of ConstantNullBlocks
         *       being fast without this. Likely the branch predictor is kicking
         *       in there. But we do this anyway, just so we don't have to trust
         *       it. It's magic. Glorious magic. But it's deep magic. And we won't
         *       always have long sequences of ConstantNullBlock. And this code
         *       shows readers we've thought about this.
         */
        return;
      }
      addRawBlock(valuesBlock);
    }
  }

  private void addRawBlock(IntBlock valuesBlock) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      SpatialExtentCartesianShapeDocValuesAggregator.combine(state, p, valuesBlock);
    }
  }

  private void addRawBlock(IntBlock valuesBlock, BooleanVector mask) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      SpatialExtentCartesianShapeDocValuesAggregator.combine(state, p, valuesBlock);
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (
      Block minXUncast = inputs.get(0).eval(page);
      Block maxXUncast = inputs.get(1).eval(page);
      Block maxYUncast = inputs.get(2).eval(page);
      Block minYUncast = inputs.get(3).eval(page);
    ) {
      if (minXUncast.areAllValuesNull()) {
        return;
      }
      IntVector minX = ((IntBlock) minXUncast).asVector();
      assert minX.getPositionCount() == 1;
      if (maxXUncast.areAllValuesNull()) {
        return;
      }
      IntVector maxX = ((IntBlock) maxXUncast).asVector();
      assert maxX.getPositionCount() == 1;
      if (maxYUncast.areAllValuesNull()) {
        return;
      }
      IntVector maxY = ((IntBlock) maxYUncast).asVector();
      assert maxY.getPositionCount() == 1;
      if (minYUncast.areAllValuesNull()) {
        return;
      }
      IntVector minY = ((IntBlock) minYUncast).asVector();
      assert minY.getPositionCount() == 1;
      SpatialExtentCartesianShapeDocValuesAggregator.combineIntermediate(state, minX.getInt(0), maxX.getInt(0), maxY.getInt(0), minY.getInt(0));
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SpatialExtentCartesianShapeDocValuesAggregator.evaluateFinal(state, driverContext);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("inputs=").append(inputs);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(
          state,
          Releasables.wrap(inputs),
          () -> driverContext.breaker().addWithoutBreaking(-ExpressionEvaluator.totalRamBytesUsed(inputs))
        );
  }
}
