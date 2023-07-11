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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link CountDistinctBytesRefAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctBytesRefGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("hll", ElementType.BYTES_REF)  );

  private final HllStates.GroupingState state;

  private final List<Integer> channels;

  private final BigArrays bigArrays;

  private final int precision;

  public CountDistinctBytesRefGroupingAggregatorFunction(List<Integer> channels,
      HllStates.GroupingState state, BigArrays bigArrays, int precision) {
    this.channels = channels;
    this.state = state;
    this.bigArrays = bigArrays;
    this.precision = precision;
  }

  public static CountDistinctBytesRefGroupingAggregatorFunction create(List<Integer> channels,
      BigArrays bigArrays, int precision) {
    return new CountDistinctBytesRefGroupingAggregatorFunction(channels, CountDistinctBytesRefAggregator.initGrouping(bigArrays, precision), bigArrays, precision);
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public GroupingAggregatorFunction.AddInput prepareProcessPage(Page page) {
    Block uncastValuesBlock = page.getBlock(channels.get(0));
    if (uncastValuesBlock.areAllValuesNull()) {
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, LongBlock groupIds) {
          addRawInputAllNulls(positionOffset, groupIds, uncastValuesBlock);
        }

        @Override
        public void add(int positionOffset, LongVector groupIds) {
          addRawInputAllNulls(positionOffset, groupIds, uncastValuesBlock);
        }
      };
    }
    BytesRefBlock valuesBlock = (BytesRefBlock) uncastValuesBlock;
    BytesRefVector valuesVector = valuesBlock.asVector();
    if (valuesVector == null) {
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, LongBlock groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }

        @Override
        public void add(int positionOffset, LongVector groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, LongBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }

      @Override
      public void add(int positionOffset, LongVector groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }
    };
  }

  private void addRawInput(int positionOffset, LongVector groups, BytesRefBlock values) {
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = Math.toIntExact(groups.getLong(groupPosition));
      if (values.isNull(groupPosition + positionOffset)) {
        state.putNull(groupId);
        continue;
      }
      int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
      int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
      for (int v = valuesStart; v < valuesEnd; v++) {
        CountDistinctBytesRefAggregator.combine(state, groupId, values.getBytesRef(v, scratch));
      }
    }
  }

  private void addRawInput(int positionOffset, LongVector groups, BytesRefVector values) {
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = Math.toIntExact(groups.getLong(groupPosition));
      CountDistinctBytesRefAggregator.combine(state, groupId, values.getBytesRef(groupPosition + positionOffset, scratch));
    }
  }

  private void addRawInputAllNulls(int positionOffset, LongVector groups, Block values) {
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = Math.toIntExact(groups.getLong(groupPosition));
      assert values.isNull(groupPosition + positionOffset);
      state.putNull(groupPosition + positionOffset);
    }
  }

  private void addRawInput(int positionOffset, LongBlock groups, BytesRefBlock values) {
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = Math.toIntExact(groups.getLong(g));
        if (values.isNull(groupPosition + positionOffset)) {
          state.putNull(groupId);
          continue;
        }
        int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
        int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
        for (int v = valuesStart; v < valuesEnd; v++) {
          CountDistinctBytesRefAggregator.combine(state, groupId, values.getBytesRef(v, scratch));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, LongBlock groups, BytesRefVector values) {
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = Math.toIntExact(groups.getLong(g));
        CountDistinctBytesRefAggregator.combine(state, groupId, values.getBytesRef(groupPosition + positionOffset, scratch));
      }
    }
  }

  private void addRawInputAllNulls(int positionOffset, LongBlock groups, Block values) {
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = Math.toIntExact(groups.getLong(g));
        assert values.isNull(groupPosition + positionOffset);
        state.putNull(groupPosition + positionOffset);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, LongVector groups, Page page) {
    assert channels.size() == intermediateBlockCount();
    BytesRefVector hll = page.<BytesRefBlock>getBlock(channels.get(0)).asVector();
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = Math.toIntExact(groups.getLong(groupPosition));
      CountDistinctBytesRefAggregator.combineIntermediate(state, groupId, hll.getBytesRef(groupPosition + positionOffset, scratch));
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    HllStates.GroupingState inState = ((CountDistinctBytesRefGroupingAggregatorFunction) input).state;
    CountDistinctBytesRefAggregator.combineStates(state, groupId, inState, position);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    state.toIntermediate(blocks, offset, selected);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected) {
    blocks[offset] = CountDistinctBytesRefAggregator.evaluateFinal(state, selected);
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
