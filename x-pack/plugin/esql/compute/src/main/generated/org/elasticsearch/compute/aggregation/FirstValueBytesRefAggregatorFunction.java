// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link FirstValueBytesRefAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class FirstValueBytesRefAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("value", ElementType.BYTES_REF),
      new IntermediateStateDesc("by", ElementType.LONG),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final FirstValueBytesRefAggregator.FirstValueLongSingleState state;

  private final List<Integer> channels;

  public FirstValueBytesRefAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      FirstValueBytesRefAggregator.FirstValueLongSingleState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static FirstValueBytesRefAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new FirstValueBytesRefAggregatorFunction(driverContext, channels, FirstValueBytesRefAggregator.initSingle(driverContext));
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
    if (mask.allTrue()) {
      // No masking
      BytesRefBlock block = page.getBlock(channels.get(0));
      BytesRefVector vector = block.asVector();
      LongBlock timestampsBlock = page.getBlock(channels.get(1));
      LongVector timestampsVector = timestampsBlock.asVector();
      if (timestampsVector == null)  {
        throw new IllegalStateException("expected @timestamp vector; but got a block");
      }
      if (vector != null) {
        addRawVector(vector, timestampsVector);
      } else {
        addRawBlock(block, timestampsVector);
      }
      return;
    }
    // Some positions masked away, others kept
    BytesRefBlock block = page.getBlock(channels.get(0));
    BytesRefVector vector = block.asVector();
    LongBlock timestampsBlock = page.getBlock(channels.get(1));
    LongVector timestampsVector = timestampsBlock.asVector();
    if (timestampsVector == null)  {
      throw new IllegalStateException("expected @timestamp vector; but got a block");
    }
    if (vector != null) {
      addRawVector(vector, timestampsVector, mask);
    } else {
      addRawBlock(block, timestampsVector, mask);
    }
  }

  private void addRawVector(BytesRefVector vector, LongVector timestamps) {
    BytesRef scratch = new BytesRef();
    for (int i = 0; i < vector.getPositionCount(); i++) {
      FirstValueBytesRefAggregator.combine(state, timestamps.getLong(i), vector.getBytesRef(i, scratch));
    }
  }

  private void addRawVector(BytesRefVector vector, LongVector timestamps, BooleanVector mask) {
    BytesRef scratch = new BytesRef();
    for (int i = 0; i < vector.getPositionCount(); i++) {
      if (mask.getBoolean(i) == false) {
        continue;
      }
      FirstValueBytesRefAggregator.combine(state, timestamps.getLong(i), vector.getBytesRef(i, scratch));
    }
  }

  private void addRawBlock(BytesRefBlock block, LongVector timestamps) {
    BytesRef scratch = new BytesRef();
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        FirstValueBytesRefAggregator.combine(state, timestamps.getLong(i), block.getBytesRef(i, scratch));
      }
    }
  }

  private void addRawBlock(BytesRefBlock block, LongVector timestamps, BooleanVector mask) {
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
        FirstValueBytesRefAggregator.combine(state, timestamps.getLong(i), block.getBytesRef(i, scratch));
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block valueUncast = page.getBlock(channels.get(0));
    if (valueUncast.areAllValuesNull()) {
      return;
    }
    BytesRefVector value = ((BytesRefBlock) valueUncast).asVector();
    assert value.getPositionCount() == 1;
    Block byUncast = page.getBlock(channels.get(1));
    if (byUncast.areAllValuesNull()) {
      return;
    }
    LongVector by = ((LongBlock) byUncast).asVector();
    assert by.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert seen.getPositionCount() == 1;
    BytesRef scratch = new BytesRef();
    FirstValueBytesRefAggregator.combineIntermediate(state, value.getBytesRef(0, scratch), by.getLong(0), seen.getBoolean(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = FirstValueBytesRefAggregator.evaluateFinal(state, driverContext);
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
