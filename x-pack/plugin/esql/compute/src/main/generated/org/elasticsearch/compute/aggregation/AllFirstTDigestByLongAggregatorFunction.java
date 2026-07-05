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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link AllFirstTDigestByLongAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class AllFirstTDigestByLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("sortKeys", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.TDIGEST),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final TDigestStates.WithLongSingleState state;

  private final List<Integer> channels;

  AllFirstTDigestByLongAggregatorFunction(DriverContext driverContext, List<Integer> channels) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = AllFirstTDigestByLongAggregator.initSingle(driverContext);
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
    TDigestBlock tdigestBlock = page.getBlock(channels.get(0));
    LongBlock sortKeyBlock = page.getBlock(channels.get(1));
    addRawBlock(tdigestBlock, sortKeyBlock, mask);
  }

  private void addRawInputNotMasked(Page page) {
    TDigestBlock tdigestBlock = page.getBlock(channels.get(0));
    LongBlock sortKeyBlock = page.getBlock(channels.get(1));
    addRawBlock(tdigestBlock, sortKeyBlock);
  }

  private void addRawBlock(TDigestBlock tdigestBlock, LongBlock sortKeyBlock) {
    TDigestHolder tdigestScratch = new TDigestHolder();
    for (int p = 0; p < tdigestBlock.getPositionCount(); p++) {
      int tdigestValueCount = tdigestBlock.getValueCount(p);
      if (tdigestValueCount == 0) {
        continue;
      }
      int sortKeyValueCount = sortKeyBlock.getValueCount(p);
      if (sortKeyValueCount == 0) {
        continue;
      }
      int tdigestStart = tdigestBlock.getFirstValueIndex(p);
      int tdigestEnd = tdigestStart + tdigestValueCount;
      for (int tdigestOffset = tdigestStart; tdigestOffset < tdigestEnd; tdigestOffset++) {
        TDigestHolder tdigestValue = tdigestBlock.getTDigestHolder(tdigestOffset, tdigestScratch);
        int sortKeyStart = sortKeyBlock.getFirstValueIndex(p);
        int sortKeyEnd = sortKeyStart + sortKeyValueCount;
        for (int sortKeyOffset = sortKeyStart; sortKeyOffset < sortKeyEnd; sortKeyOffset++) {
          long sortKeyValue = sortKeyBlock.getLong(sortKeyOffset);
          AllFirstTDigestByLongAggregator.combine(state, tdigestValue, sortKeyValue);
        }
      }
    }
  }

  private void addRawBlock(TDigestBlock tdigestBlock, LongBlock sortKeyBlock, BooleanVector mask) {
    TDigestHolder tdigestScratch = new TDigestHolder();
    for (int p = 0; p < tdigestBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int tdigestValueCount = tdigestBlock.getValueCount(p);
      if (tdigestValueCount == 0) {
        continue;
      }
      int sortKeyValueCount = sortKeyBlock.getValueCount(p);
      if (sortKeyValueCount == 0) {
        continue;
      }
      int tdigestStart = tdigestBlock.getFirstValueIndex(p);
      int tdigestEnd = tdigestStart + tdigestValueCount;
      for (int tdigestOffset = tdigestStart; tdigestOffset < tdigestEnd; tdigestOffset++) {
        TDigestHolder tdigestValue = tdigestBlock.getTDigestHolder(tdigestOffset, tdigestScratch);
        int sortKeyStart = sortKeyBlock.getFirstValueIndex(p);
        int sortKeyEnd = sortKeyStart + sortKeyValueCount;
        for (int sortKeyOffset = sortKeyStart; sortKeyOffset < sortKeyEnd; sortKeyOffset++) {
          long sortKeyValue = sortKeyBlock.getLong(sortKeyOffset);
          AllFirstTDigestByLongAggregator.combine(state, tdigestValue, sortKeyValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block sortKeysUncast = page.getBlock(channels.get(0));
    LongBlock sortKeys = (LongBlock) sortKeysUncast;
    assert sortKeys.getPositionCount() == 1;
    Block valuesUncast = page.getBlock(channels.get(1));
    TDigestBlock values = (TDigestBlock) valuesUncast;
    assert values.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(2));
    BooleanBlock seen = (BooleanBlock) seenUncast;
    assert seen.getPositionCount() == 1;
    TDigestHolder valuesScratch = new TDigestHolder();
    AllFirstTDigestByLongAggregator.combineIntermediate(state, sortKeys.getLong(0), values, seen.getBoolean(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = AllFirstTDigestByLongAggregator.evaluateFinal(state, driverContext);
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
