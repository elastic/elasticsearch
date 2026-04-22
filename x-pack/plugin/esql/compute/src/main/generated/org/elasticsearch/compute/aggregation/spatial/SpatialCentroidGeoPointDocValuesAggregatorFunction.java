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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;

/**
 * {@link AggregatorFunction} implementation for {@link SpatialCentroidGeoPointDocValuesAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class SpatialCentroidGeoPointDocValuesAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("xVal", ElementType.DOUBLE),
      new IntermediateStateDesc("xDel", ElementType.DOUBLE),
      new IntermediateStateDesc("yVal", ElementType.DOUBLE),
      new IntermediateStateDesc("yDel", ElementType.DOUBLE),
      new IntermediateStateDesc("count", ElementType.LONG)  );

  private final DriverContext driverContext;

  private final CentroidPointAggregator.CentroidState state;

  private final List<ExpressionEvaluator> inputs;

  private final CoordinateEncoder encoder;

  SpatialCentroidGeoPointDocValuesAggregatorFunction(DriverContext driverContext,
      List<ExpressionEvaluator> inputs, CoordinateEncoder encoder) {
    this.encoder = encoder;
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = SpatialCentroidGeoPointDocValuesAggregator.initSingle(encoder);
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
    try (Block vUncast = inputs.get(0).eval(page)) {
      LongBlock vBlock = (LongBlock) vUncast;
      LongVector vVector = vBlock.asVector();
      if (vVector == null) {
        if (vBlock.areAllValuesNull()) {
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
        addRawBlock(vBlock, mask);
        return;
      }
      addRawVector(vVector, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (Block vUncast = inputs.get(0).eval(page)) {
      LongBlock vBlock = (LongBlock) vUncast;
      LongVector vVector = vBlock.asVector();
      if (vVector == null) {
        if (vBlock.areAllValuesNull()) {
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
        addRawBlock(vBlock);
        return;
      }
      addRawVector(vVector);
    }
  }

  private void addRawVector(LongVector vVector) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      long vValue = vVector.getLong(valuesPosition);
      SpatialCentroidGeoPointDocValuesAggregator.combine(state, vValue);
    }
  }

  private void addRawVector(LongVector vVector, BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      long vValue = vVector.getLong(valuesPosition);
      SpatialCentroidGeoPointDocValuesAggregator.combine(state, vValue);
    }
  }

  private void addRawBlock(LongBlock vBlock) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      int vValueCount = vBlock.getValueCount(p);
      if (vValueCount == 0) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vValueCount;
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        long vValue = vBlock.getLong(vOffset);
        SpatialCentroidGeoPointDocValuesAggregator.combine(state, vValue);
      }
    }
  }

  private void addRawBlock(LongBlock vBlock, BooleanVector mask) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int vValueCount = vBlock.getValueCount(p);
      if (vValueCount == 0) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vValueCount;
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        long vValue = vBlock.getLong(vOffset);
        SpatialCentroidGeoPointDocValuesAggregator.combine(state, vValue);
      }
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
      Block countUncast = inputs.get(4).eval(page);
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
      if (countUncast.areAllValuesNull()) {
        return;
      }
      LongVector count = ((LongBlock) countUncast).asVector();
      assert count.getPositionCount() == 1;
      SpatialCentroidGeoPointDocValuesAggregator.combineIntermediate(state, xVal.getDouble(0), xDel.getDouble(0), yVal.getDouble(0), yDel.getDouble(0), count.getLong(0));
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SpatialCentroidGeoPointDocValuesAggregator.evaluateFinal(state, driverContext);
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
