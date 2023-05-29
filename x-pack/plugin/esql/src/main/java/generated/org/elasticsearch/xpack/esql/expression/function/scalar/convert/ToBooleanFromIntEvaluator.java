// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import java.util.BitSet;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanArrayBlock;
import org.elasticsearch.compute.data.BooleanArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.ConstantBooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToBoolean}.
 * This class is generated. Do not edit it.
 */
public final class ToBooleanFromIntEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  public ToBooleanFromIntEvaluator(EvalOperator.ExpressionEvaluator field, Source source) {
    super(field, source);
  }

  @Override
  public String name() {
    return "ToBoolean";
  }

  @Override
  public Block evalVector(Vector v) {
    IntVector vector = (IntVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      try {
        return new ConstantBooleanVector(evalValue(vector, 0), positionCount).asBlock();
      } catch (Exception e) {
        registerException(e);
        return Block.constantNullBlock(positionCount);
      }
    }
    BitSet nullsMask = null;
    boolean[] values = new boolean[positionCount];
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
          ? new BooleanArrayVector(values, positionCount).asBlock()
          // UNORDERED, since whatever ordering there is, it isn't necessarily preserved
          : new BooleanArrayBlock(values, positionCount, null, nullsMask, Block.MvOrdering.UNORDERED);
  }

  private static boolean evalValue(IntVector container, int index) {
    int value = container.getInt(index);
    return ToBoolean.fromInt(value);
  }

  @Override
  public Block evalBlock(Block b) {
    IntBlock block = (IntBlock) b;
    int positionCount = block.getPositionCount();
    BooleanBlock.Builder builder = BooleanBlock.newBlockBuilder(positionCount);
    for (int p = 0; p < positionCount; p++) {
      int valueCount = block.getValueCount(p);
      int start = block.getFirstValueIndex(p);
      int end = start + valueCount;
      boolean positionOpened = false;
      boolean valuesAppended = false;
      for (int i = start; i < end; i++) {
        try {
          boolean value = evalValue(block, i);
          if (positionOpened == false && valueCount > 1) {
            builder.beginPositionEntry();
            positionOpened = true;
          }
          builder.appendBoolean(value);
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

  private static boolean evalValue(IntBlock container, int index) {
    int value = container.getInt(index);
    return ToBoolean.fromInt(value);
  }
}
