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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * {@link AggregatorFunction} implementation for {@link SpatialExtentGeoPointDocValuesAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class SpatialExtentGeoPointDocValuesAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.INT),
      new IntermediateStateDesc("bottom", ElementType.INT),
      new IntermediateStateDesc("negLeft", ElementType.INT),
      new IntermediateStateDesc("negRight", ElementType.INT),
      new IntermediateStateDesc("posLeft", ElementType.INT),
      new IntermediateStateDesc("posRight", ElementType.INT)  );

  private final DriverContext driverContext;

  private final SpatialExtentStateWrappedLongitudeState state;

  private final List<ExpressionEvaluator> inputs;

  SpatialExtentGeoPointDocValuesAggregatorFunction(DriverContext driverContext,
      List<ExpressionEvaluator> inputs) {
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = SpatialExtentGeoPointDocValuesAggregator.initSingle();
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
    try (Block encodedUncast = inputs.get(0).eval(page)) {
      LongBlock encodedBlock = (LongBlock) encodedUncast;
      LongVector encodedVector = encodedBlock.asVector();
      if (encodedVector == null) {
        if (encodedBlock.areAllValuesNull()) {
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
        addRawBlock(encodedBlock, mask);
        return;
      }
      addRawVector(encodedVector, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (Block encodedUncast = inputs.get(0).eval(page)) {
      LongBlock encodedBlock = (LongBlock) encodedUncast;
      LongVector encodedVector = encodedBlock.asVector();
      if (encodedVector == null) {
        if (encodedBlock.areAllValuesNull()) {
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
        addRawBlock(encodedBlock);
        return;
      }
      addRawVector(encodedVector);
    }
  }

  private void addRawVector(LongVector encodedVector) {
    for (int valuesPosition = 0; valuesPosition < encodedVector.getPositionCount(); valuesPosition++) {
      long encodedValue = encodedVector.getLong(valuesPosition);
      SpatialExtentGeoPointDocValuesAggregator.combine(state, encodedValue);
    }
  }

  private void addRawVector(LongVector encodedVector, BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < encodedVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      long encodedValue = encodedVector.getLong(valuesPosition);
      SpatialExtentGeoPointDocValuesAggregator.combine(state, encodedValue);
    }
  }

  private void addRawBlock(LongBlock encodedBlock) {
    for (int p = 0; p < encodedBlock.getPositionCount(); p++) {
      int encodedValueCount = encodedBlock.getValueCount(p);
      if (encodedValueCount == 0) {
        continue;
      }
      int encodedStart = encodedBlock.getFirstValueIndex(p);
      int encodedEnd = encodedStart + encodedValueCount;
      for (int encodedOffset = encodedStart; encodedOffset < encodedEnd; encodedOffset++) {
        long encodedValue = encodedBlock.getLong(encodedOffset);
        SpatialExtentGeoPointDocValuesAggregator.combine(state, encodedValue);
      }
    }
  }

  private void addRawBlock(LongBlock encodedBlock, BooleanVector mask) {
    for (int p = 0; p < encodedBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int encodedValueCount = encodedBlock.getValueCount(p);
      if (encodedValueCount == 0) {
        continue;
      }
      int encodedStart = encodedBlock.getFirstValueIndex(p);
      int encodedEnd = encodedStart + encodedValueCount;
      for (int encodedOffset = encodedStart; encodedOffset < encodedEnd; encodedOffset++) {
        long encodedValue = encodedBlock.getLong(encodedOffset);
        SpatialExtentGeoPointDocValuesAggregator.combine(state, encodedValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (
      Block topUncast = inputs.get(0).eval(page);
      Block bottomUncast = inputs.get(1).eval(page);
      Block negLeftUncast = inputs.get(2).eval(page);
      Block negRightUncast = inputs.get(3).eval(page);
      Block posLeftUncast = inputs.get(4).eval(page);
      Block posRightUncast = inputs.get(5).eval(page);
    ) {
      if (topUncast.areAllValuesNull()) {
        return;
      }
      IntVector top = ((IntBlock) topUncast).asVector();
      assert top.getPositionCount() == 1;
      if (bottomUncast.areAllValuesNull()) {
        return;
      }
      IntVector bottom = ((IntBlock) bottomUncast).asVector();
      assert bottom.getPositionCount() == 1;
      if (negLeftUncast.areAllValuesNull()) {
        return;
      }
      IntVector negLeft = ((IntBlock) negLeftUncast).asVector();
      assert negLeft.getPositionCount() == 1;
      if (negRightUncast.areAllValuesNull()) {
        return;
      }
      IntVector negRight = ((IntBlock) negRightUncast).asVector();
      assert negRight.getPositionCount() == 1;
      if (posLeftUncast.areAllValuesNull()) {
        return;
      }
      IntVector posLeft = ((IntBlock) posLeftUncast).asVector();
      assert posLeft.getPositionCount() == 1;
      if (posRightUncast.areAllValuesNull()) {
        return;
      }
      IntVector posRight = ((IntBlock) posRightUncast).asVector();
      assert posRight.getPositionCount() == 1;
      SpatialExtentGeoPointDocValuesAggregator.combineIntermediate(state, top.getInt(0), bottom.getInt(0), negLeft.getInt(0), negRight.getInt(0), posLeft.getInt(0), posRight.getInt(0));
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SpatialExtentGeoPointDocValuesAggregator.evaluateFinal(state, driverContext);
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
