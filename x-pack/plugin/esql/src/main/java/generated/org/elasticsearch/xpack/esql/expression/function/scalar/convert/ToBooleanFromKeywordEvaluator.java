// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ConstantBooleanVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToBoolean}.
 * This class is generated. Do not edit it.
 */
public final class ToBooleanFromKeywordEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  public ToBooleanFromKeywordEvaluator(EvalOperator.ExpressionEvaluator field) {
    super(field);
  }

  @Override
  public String name() {
    return "ToBoolean";
  }

  @Override
  public Vector evalVector(Vector v) {
    BytesRefVector vector = (BytesRefVector) v;
    int positionCount = v.getPositionCount();
    BytesRef scratchPad = new BytesRef();
    if (vector.isConstant()) {
      return new ConstantBooleanVector(evalValue(vector, 0, scratchPad), positionCount);
    }
    boolean[] values = new boolean[positionCount];
    for (int p = 0; p < positionCount; p++) {
      values[p] = evalValue(vector, p, scratchPad);
    }
    return new BooleanArrayVector(values, positionCount);
  }

  private static boolean evalValue(BytesRefVector container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return ToBoolean.fromKeyword(value);
  }

  @Override
  public Block evalBlock(Block b) {
    BytesRefBlock block = (BytesRefBlock) b;
    int positionCount = block.getPositionCount();
    BooleanBlock.Builder builder = BooleanBlock.newBlockBuilder(positionCount);
    BytesRef scratchPad = new BytesRef();
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
        builder.appendBoolean(evalValue(block, i, scratchPad));
      }
      builder.endPositionEntry();
    }
    return builder.build();
  }

  private static boolean evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return ToBoolean.fromKeyword(value);
  }
}
