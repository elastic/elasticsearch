// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import java.util.BitSet;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantIntVector;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToInteger}.
 * This class is generated. Do not edit it.
 */
public final class ToIntegerFromLongEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  public ToIntegerFromLongEvaluator(EvalOperator.ExpressionEvaluator field, Source source) {
    super(field, source);
  }

  @Override
  public String name() {
    return "ToInteger";
  }

  @Override
  public Block evalVector(Vector v) {
    LongVector vector = (LongVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      try {
        return new ConstantIntVector(evalValue(vector, 0), positionCount).asBlock();
      } catch (Exception e) {
        registerException(e);
        return Block.constantNullBlock(positionCount);
      }
    }
    BitSet nullsMask = null;
    int[] values = new int[positionCount];
    for (int p = 0; p < positionCount; p++) {
      try {
        values[p] = evalValue(vector, p);
      } catch (Exception e) {
        registerException(e);
        if (nullsMask == null) {
          nullsMask = new BitSet(positionCount);
        }
        nullsMask.set(p);
      }
    }
    return nullsMask == null
          ? new IntArrayVector(values, positionCount).asBlock()
          // UNORDERED, since whatever ordering there is, it isn't necessarily preserved
          : new IntArrayBlock(values, positionCount, null, nullsMask, Block.MvOrdering.UNORDERED);
  }

  private static int evalValue(LongVector container, int index) {
    long value = container.getLong(index);
    return ToInteger.fromLong(value);
  }

  @Override
  public Block evalBlock(Block b) {
    LongBlock block = (LongBlock) b;
    int positionCount = block.getPositionCount();
    IntBlock.Builder builder = IntBlock.newBlockBuilder(positionCount);
    for (int p = 0; p < positionCount; p++) {
      int valueCount = block.getValueCount(p);
      int start = block.getFirstValueIndex(p);
      int end = start + valueCount;
      boolean positionOpened = false;
      boolean valuesAppended = false;
      for (int i = start; i < end; i++) {
        try {
          int value = evalValue(block, i);
          if (positionOpened == false && valueCount > 1) {
            builder.beginPositionEntry();
            positionOpened = true;
          }
          builder.appendInt(value);
          valuesAppended = true;
        } catch (Exception e) {
          registerException(e);
        }
      }
      if (valuesAppended == false) {
        builder.appendNull();
      } else if (positionOpened) {
        builder.endPositionEntry();
      }
    }
    return builder.build();
  }

  private static int evalValue(LongBlock container, int index) {
    long value = container.getLong(index);
    return ToInteger.fromLong(value);
  }
}
