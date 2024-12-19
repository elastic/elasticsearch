// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation.spatial;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link SpatialExtentGeoPointSourceValuesAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SpatialExtentGeoPointSourceValuesAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("minNegX", ElementType.INT),
      new IntermediateStateDesc("minPosX", ElementType.INT),
      new IntermediateStateDesc("maxNegX", ElementType.INT),
      new IntermediateStateDesc("maxPosX", ElementType.INT),
      new IntermediateStateDesc("maxY", ElementType.INT),
      new IntermediateStateDesc("minY", ElementType.INT)  );

  private final DriverContext driverContext;

  private final SpatialExtentStateWrappedLongitudeState state;

  private final List<Integer> channels;

  public SpatialExtentGeoPointSourceValuesAggregatorFunction(DriverContext driverContext,
      List<Integer> channels, SpatialExtentStateWrappedLongitudeState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static SpatialExtentGeoPointSourceValuesAggregatorFunction create(
      DriverContext driverContext, List<Integer> channels) {
    return new SpatialExtentGeoPointSourceValuesAggregatorFunction(driverContext, channels, SpatialExtentGeoPointSourceValuesAggregator.initSingle());
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
      return;
    }
    BytesRefBlock block = page.getBlock(channels.get(0));
    BytesRefVector vector = block.asVector();
    if (mask.allTrue()) {
      // No masking
      if (vector != null) {
        addRawVector(vector);
      } else {
        addRawBlock(block);
      }
      return;
    } else {
      // Some positions masked away, others kept
      if (vector != null) {
        addRawVector(vector, mask);
      } else {
        addRawBlock(block, mask);
      }
    }
  }

  private void addRawVector(BytesRefVector vector) {
    BytesRef scratch = new BytesRef();
    for (int i = 0; i < vector.getPositionCount(); i++) {
      SpatialExtentGeoPointSourceValuesAggregator.combine(state, vector.getBytesRef(i, scratch));
    }
  }

  private void addRawVector(BytesRefVector vector, BooleanVector mask) {
    BytesRef scratch = new BytesRef();
    for (int i = 0; i < vector.getPositionCount(); i++) {
      if (mask.getBoolean(i) == false) {
        continue;
      }
      SpatialExtentGeoPointSourceValuesAggregator.combine(state, vector.getBytesRef(i, scratch));
    }
  }

  private void addRawBlock(BytesRefBlock block) {
    BytesRef scratch = new BytesRef();
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        SpatialExtentGeoPointSourceValuesAggregator.combine(state, block.getBytesRef(i, scratch));
      }
    }
  }

  private void addRawBlock(BytesRefBlock block, BooleanVector mask) {
    BytesRef scratch = new BytesRef();
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        SpatialExtentGeoPointSourceValuesAggregator.combine(state, block.getBytesRef(i, scratch));
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block minNegXUncast = page.getBlock(channels.get(0));
    if (minNegXUncast.areAllValuesNull()) {
      return;
    }
    IntVector minNegX = ((IntBlock) minNegXUncast).asVector();
    assert minNegX.getPositionCount() == 1;
    Block minPosXUncast = page.getBlock(channels.get(1));
    if (minPosXUncast.areAllValuesNull()) {
      return;
    }
    IntVector minPosX = ((IntBlock) minPosXUncast).asVector();
    assert minPosX.getPositionCount() == 1;
    Block maxNegXUncast = page.getBlock(channels.get(2));
    if (maxNegXUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxNegX = ((IntBlock) maxNegXUncast).asVector();
    assert maxNegX.getPositionCount() == 1;
    Block maxPosXUncast = page.getBlock(channels.get(3));
    if (maxPosXUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxPosX = ((IntBlock) maxPosXUncast).asVector();
    assert maxPosX.getPositionCount() == 1;
    Block maxYUncast = page.getBlock(channels.get(4));
    if (maxYUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxY = ((IntBlock) maxYUncast).asVector();
    assert maxY.getPositionCount() == 1;
    Block minYUncast = page.getBlock(channels.get(5));
    if (minYUncast.areAllValuesNull()) {
      return;
    }
    IntVector minY = ((IntBlock) minYUncast).asVector();
    assert minY.getPositionCount() == 1;
    SpatialExtentGeoPointSourceValuesAggregator.combineIntermediate(state, minNegX.getInt(0), minPosX.getInt(0), maxNegX.getInt(0), maxPosX.getInt(0), maxY.getInt(0), minY.getInt(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SpatialExtentGeoPointSourceValuesAggregator.evaluateFinal(state, driverContext);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("channels=").append(channels);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    state.close();
  }
}
