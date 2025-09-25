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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link TopBytesRefLongAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class TopBytesRefLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.BYTES_REF),
      new IntermediateStateDesc("extra", ElementType.LONG)  );

  private final DriverContext driverContext;

  private final TopBytesRefLongAggregator.SingleState state;

  private final List<Integer> channels;

  private final int limit;

  private final boolean ascending;

  public TopBytesRefLongAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      TopBytesRefLongAggregator.SingleState state, int limit, boolean ascending) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
    this.limit = limit;
    this.ascending = ascending;
  }

  public static TopBytesRefLongAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels, int limit, boolean ascending) {
    return new TopBytesRefLongAggregatorFunction(driverContext, channels, TopBytesRefLongAggregator.initSingle(driverContext.bigArrays(), limit, ascending), limit, ascending);
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
    LongBlock extraBlock = page.getBlock(channels.get(1));
    BytesRefVector vVector = vBlock.asVector();
    if (vVector == null) {
      addRawBlock(vBlock, extraBlock, mask);
      return;
    }
    LongVector extraVector = extraBlock.asVector();
    if (extraVector == null) {
      addRawBlock(vBlock, extraBlock, mask);
      return;
    }
    addRawVector(vVector, extraVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    BytesRefBlock vBlock = page.getBlock(channels.get(0));
    LongBlock extraBlock = page.getBlock(channels.get(1));
    BytesRefVector vVector = vBlock.asVector();
    if (vVector == null) {
      addRawBlock(vBlock, extraBlock);
      return;
    }
    LongVector extraVector = extraBlock.asVector();
    if (extraVector == null) {
      addRawBlock(vBlock, extraBlock);
      return;
    }
    addRawVector(vVector, extraVector);
  }

  private void addRawVector(BytesRefVector vVector, LongVector extraVector) {
    BytesRef vScratch = new BytesRef();
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      BytesRef vValue = vVector.getBytesRef(valuesPosition, vScratch);
      long extraValue = extraVector.getLong(valuesPosition);
      TopBytesRefLongAggregator.combine(state, vValue, extraValue);
    }
  }

  private void addRawVector(BytesRefVector vVector, LongVector extraVector, BooleanVector mask) {
    BytesRef vScratch = new BytesRef();
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      BytesRef vValue = vVector.getBytesRef(valuesPosition, vScratch);
      long extraValue = extraVector.getLong(valuesPosition);
      TopBytesRefLongAggregator.combine(state, vValue, extraValue);
    }
  }

  private void addRawBlock(BytesRefBlock vBlock, LongBlock extraBlock) {
    BytesRef vScratch = new BytesRef();
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      if (vBlock.isNull(p)) {
        continue;
      }
      if (extraBlock.isNull(p)) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vBlock.getValueCount(p);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        BytesRef vValue = vBlock.getBytesRef(vOffset, vScratch);
        int extraStart = extraBlock.getFirstValueIndex(p);
        int extraEnd = extraStart + extraBlock.getValueCount(p);
        for (int extraOffset = extraStart; extraOffset < extraEnd; extraOffset++) {
          long extraValue = extraBlock.getLong(extraOffset);
          TopBytesRefLongAggregator.combine(state, vValue, extraValue);
        }
      }
    }
  }

  private void addRawBlock(BytesRefBlock vBlock, LongBlock extraBlock, BooleanVector mask) {
    BytesRef vScratch = new BytesRef();
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      if (vBlock.isNull(p)) {
        continue;
      }
      if (extraBlock.isNull(p)) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vBlock.getValueCount(p);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        BytesRef vValue = vBlock.getBytesRef(vOffset, vScratch);
        int extraStart = extraBlock.getFirstValueIndex(p);
        int extraEnd = extraStart + extraBlock.getValueCount(p);
        for (int extraOffset = extraStart; extraOffset < extraEnd; extraOffset++) {
          long extraValue = extraBlock.getLong(extraOffset);
          TopBytesRefLongAggregator.combine(state, vValue, extraValue);
        }
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
    Block extraUncast = page.getBlock(channels.get(1));
    if (extraUncast.areAllValuesNull()) {
      return;
    }
    LongBlock extra = (LongBlock) extraUncast;
    assert extra.getPositionCount() == 1;
    BytesRef scratch = new BytesRef();
    TopBytesRefLongAggregator.combineIntermediate(state, top, extra);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = TopBytesRefLongAggregator.evaluateFinal(state, driverContext);
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
