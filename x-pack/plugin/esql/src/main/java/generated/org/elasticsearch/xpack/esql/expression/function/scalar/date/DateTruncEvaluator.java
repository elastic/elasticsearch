// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateTrunc}.
 * This class is generated. Do not edit it.
 */
public final class DateTruncEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator fieldVal;

  private final Rounding.Prepared rounding;

  private final DriverContext driverContext;

  public DateTruncEvaluator(EvalOperator.ExpressionEvaluator fieldVal, Rounding.Prepared rounding,
      DriverContext driverContext) {
    this.fieldVal = fieldVal;
    this.rounding = rounding;
    this.driverContext = driverContext;
  }

  @Override
  public Block.Ref eval(Page page) {
    try (Block.Ref fieldValRef = fieldVal.eval(page)) {
      if (fieldValRef.block().areAllValuesNull()) {
        return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
      }
      LongBlock fieldValBlock = (LongBlock) fieldValRef.block();
      LongVector fieldValVector = fieldValBlock.asVector();
      if (fieldValVector == null) {
        return Block.Ref.floating(eval(page.getPositionCount(), fieldValBlock));
      }
      return Block.Ref.floating(eval(page.getPositionCount(), fieldValVector).asBlock());
    }
  }

  public LongBlock eval(int positionCount, LongBlock fieldValBlock) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (fieldValBlock.isNull(p) || fieldValBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendLong(DateTrunc.process(fieldValBlock.getLong(fieldValBlock.getFirstValueIndex(p)), rounding));
    }
    return result.build();
  }

  public LongVector eval(int positionCount, LongVector fieldValVector) {
    LongVector.Builder result = LongVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendLong(DateTrunc.process(fieldValVector.getLong(p), rounding));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "DateTruncEvaluator[" + "fieldVal=" + fieldVal + ", rounding=" + rounding + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(fieldVal);
  }
}
