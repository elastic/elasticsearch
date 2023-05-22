// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefArrayVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ConstantBytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToString}.
 * This class is generated. Do not edit it.
 */
public final class ToStringFromDatetimeEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  public ToStringFromDatetimeEvaluator(EvalOperator.ExpressionEvaluator field) {
    super(field);
  }

  @Override
  public String name() {
    return "ToString";
  }

  @Override
  public Vector evalVector(Vector v) {
    LongVector vector = (LongVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      return new ConstantBytesRefVector(evalValue(vector, 0), positionCount);
    }
    BytesRefArray values = new BytesRefArray(positionCount, BigArrays.NON_RECYCLING_INSTANCE);
    for (int p = 0; p < positionCount; p++) {
      values.append(evalValue(vector, p));
    }
    return new BytesRefArrayVector(values, positionCount);
  }

  private static BytesRef evalValue(LongVector container, int index) {
    long value = container.getLong(index);
    return ToString.fromDatetime(value);
  }

  @Override
  public Block evalBlock(Block b) {
    LongBlock block = (LongBlock) b;
    int positionCount = block.getPositionCount();
    BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(positionCount);
    for (int p = 0; p < positionCount; p++) {
      int valueCount = block.getValueCount(p);
      if (valueCount == 0) {
        builder.appendNull();
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + valueCount;
      builder.beginPositionEntry();
      for (int i = start; i < end; i++) {
        builder.appendBytesRef(evalValue(block, i));
      }
      builder.endPositionEntry();
    }
    return builder.build();
  }

  private static BytesRef evalValue(LongBlock container, int index) {
    long value = container.getLong(index);
    return ToString.fromDatetime(value);
  }
}
