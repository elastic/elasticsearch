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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;

/**
 * {@link AggregatorFunction} implementation for {@link SpatialCentroidShapeCombinedDocValuesAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class SpatialCentroidShapeCombinedDocValuesAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("xVal", ElementType.DOUBLE),
      new IntermediateStateDesc("xDel", ElementType.DOUBLE),
      new IntermediateStateDesc("yVal", ElementType.DOUBLE),
      new IntermediateStateDesc("yDel", ElementType.DOUBLE),
      new IntermediateStateDesc("weight", ElementType.DOUBLE),
      new IntermediateStateDesc("shapeType", ElementType.INT)  );

  private final DriverContext driverContext;

  private final CentroidShapeAggregator.ShapeCentroidState state;

  private final List<ExpressionEvaluator> inputs;

  private final CoordinateEncoder encoder;

  SpatialCentroidShapeCombinedDocValuesAggregatorFunction(DriverContext driverContext,
      List<ExpressionEvaluator> inputs, CoordinateEncoder encoder) {
    this.encoder = encoder;
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = SpatialCentroidShapeCombinedDocValuesAggregator.initSingle(encoder);
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
      DoubleBlock valuesBlock = (DoubleBlock) valuesUncast;
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
      DoubleBlock valuesBlock = (DoubleBlock) valuesUncast;
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

  private void addRawBlock(DoubleBlock valuesBlock) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      SpatialCentroidShapeCombinedDocValuesAggregator.combine(state, p, valuesBlock);
    }
  }

  private void addRawBlock(DoubleBlock valuesBlock, BooleanVector mask) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      SpatialCentroidShapeCombinedDocValuesAggregator.combine(state, p, valuesBlock);
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (
      Block xValUncast = inputs.get(0).eval(page);
      Block xDelUncast = inputs.get(1).eval(page);
      Block yValUncast = inputs.get(2).eval(page);
      Block yDelUncast = inputs.get(3).eval(page);
      Block weightUncast = inputs.get(4).eval(page);
      Block shapeTypeUncast = inputs.get(5).eval(page);
    ) {
      if (xValUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector xVal = ((DoubleBlock) xValUncast).asVector();
      assert xVal.getPositionCount() == 1;
      if (xDelUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector xDel = ((DoubleBlock) xDelUncast).asVector();
      assert xDel.getPositionCount() == 1;
      if (yValUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector yVal = ((DoubleBlock) yValUncast).asVector();
      assert yVal.getPositionCount() == 1;
      if (yDelUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector yDel = ((DoubleBlock) yDelUncast).asVector();
      assert yDel.getPositionCount() == 1;
      if (weightUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector weight = ((DoubleBlock) weightUncast).asVector();
      assert weight.getPositionCount() == 1;
      if (shapeTypeUncast.areAllValuesNull()) {
        return;
      }
      IntVector shapeType = ((IntBlock) shapeTypeUncast).asVector();
      assert shapeType.getPositionCount() == 1;
      SpatialCentroidShapeCombinedDocValuesAggregator.combineIntermediate(state, xVal.getDouble(0), xDel.getDouble(0), yVal.getDouble(0), yDel.getDouble(0), weight.getDouble(0), shapeType.getInt(0));
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SpatialCentroidShapeCombinedDocValuesAggregator.evaluateFinal(state, driverContext);
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
