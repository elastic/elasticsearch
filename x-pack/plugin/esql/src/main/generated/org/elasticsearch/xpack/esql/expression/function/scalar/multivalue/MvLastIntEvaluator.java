// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvLast}.
 * This class is generated. Edit {@code MvEvaluatorImplementer} instead.
 */
public final class MvLastIntEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvLastIntEvaluator(EvalOperator.ExpressionEvaluator field, DriverContext driverContext) {
    super(driverContext, field);
  }

  @Override
  public String name() {
    return "MvLast";
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block evalNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        int end = first + valueCount;
        int result = MvLast.process(v, first, end);
        builder.appendInt(result);
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block evalNotNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (IntVector.FixedBuilder builder = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        int end = first + valueCount;
        int result = MvLast.process(v, first, end);
        builder.appendInt(result);
      }
      return builder.build().asBlock();
    }
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory field;

    public Factory(EvalOperator.ExpressionEvaluator.Factory field) {
      this.field = field;
    }

    @Override
    public MvLastIntEvaluator get(DriverContext context) {
      return new MvLastIntEvaluator(field.get(context), context);
    }

    @Override
    public String toString() {
      return "MvLast[field=" + field + "]";
    }
  }
}
