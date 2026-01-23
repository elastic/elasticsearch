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
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link TopBytesRefAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class TopBytesRefAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.BYTES_REF)  );

  private final DriverContext driverContext;

  private final TopBytesRefAggregator.SingleState state;

  private final List<Integer> channels;

  private final int limit;

  private final boolean ascending;

  public TopBytesRefAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      TopBytesRefAggregator.SingleState state, int limit, boolean ascending) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
    this.limit = limit;
    this.ascending = ascending;
  }

  public static TopBytesRefAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels, int limit, boolean ascending) {
    return new TopBytesRefAggregatorFunction(driverContext, channels, TopBytesRefAggregator.initSingle(driverContext.bigArrays(), limit, ascending), limit, ascending);
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
    BytesRefBlock vBlock = page.getBlock(channels.get(0));
    BytesRefVector vVector = vBlock.asVector();
    if (vVector == null) {
      addRawBlock(vBlock, mask);
      return;
    }
    addRawVector(vVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    BytesRefBlock vBlock = page.getBlock(channels.get(0));
    BytesRefVector vVector = vBlock.asVector();
    if (vVector == null) {
      addRawBlock(vBlock);
      return;
    }
    addRawVector(vVector);
  }

  private void addRawVector(BytesRefVector vVector) {
    BytesRef vScratch = new BytesRef();
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      BytesRef vValue = vVector.getBytesRef(valuesPosition, vScratch);
      TopBytesRefAggregator.combine(state, vValue);
    }
  }

  private void addRawVector(BytesRefVector vVector, BooleanVector mask) {
    BytesRef vScratch = new BytesRef();
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      BytesRef vValue = vVector.getBytesRef(valuesPosition, vScratch);
      TopBytesRefAggregator.combine(state, vValue);
    }
  }

  private void addRawBlock(BytesRefBlock vBlock) {
    BytesRef vScratch = new BytesRef();
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      int vValueCount = vBlock.getValueCount(p);
      if (vValueCount == 0) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vValueCount;
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        BytesRef vValue = vBlock.getBytesRef(vOffset, vScratch);
        TopBytesRefAggregator.combine(state, vValue);
      }
    }
  }

  private void addRawBlock(BytesRefBlock vBlock, BooleanVector mask) {
    BytesRef vScratch = new BytesRef();
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
        BytesRef vValue = vBlock.getBytesRef(vOffset, vScratch);
        TopBytesRefAggregator.combine(state, vValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
      return;
    }
    BytesRefBlock top = (BytesRefBlock) topUncast;
    assert top.getPositionCount() == 1;
    BytesRef topScratch = new BytesRef();
    TopBytesRefAggregator.combineIntermediate(state, top);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = TopBytesRefAggregator.evaluateFinal(state, driverContext);
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
