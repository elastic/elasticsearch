// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StDWithin}.
 * This class is generated. Do not edit it.
 */
public final class StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final EvalOperator.ExpressionEvaluator rightValue;

  private final EvalOperator.ExpressionEvaluator argValue;

  private final DriverContext driverContext;

  public StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, EvalOperator.ExpressionEvaluator rightValue,
      EvalOperator.ExpressionEvaluator argValue, DriverContext driverContext) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.argValue = argValue;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock leftValueBlock = (LongBlock) leftValue.eval(page)) {
      try (BytesRefBlock rightValueBlock = (BytesRefBlock) rightValue.eval(page)) {
        try (DoubleBlock argValueBlock = (DoubleBlock) argValue.eval(page)) {
          LongVector leftValueVector = leftValueBlock.asVector();
          if (leftValueVector == null) {
            return eval(page.getPositionCount(), leftValueBlock, rightValueBlock, argValueBlock);
          }
          BytesRefVector rightValueVector = rightValueBlock.asVector();
          if (rightValueVector == null) {
            return eval(page.getPositionCount(), leftValueBlock, rightValueBlock, argValueBlock);
          }
          DoubleVector argValueVector = argValueBlock.asVector();
          if (argValueVector == null) {
            return eval(page.getPositionCount(), leftValueBlock, rightValueBlock, argValueBlock);
          }
          return eval(page.getPositionCount(), leftValueVector, rightValueVector, argValueVector).asBlock();
        }
      }
    }
  }

  public BooleanBlock eval(int positionCount, LongBlock leftValueBlock,
      BytesRefBlock rightValueBlock, DoubleBlock argValueBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef rightValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (leftValueBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (leftValueBlock.getValueCount(p) != 1) {
          if (leftValueBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (rightValueBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (rightValueBlock.getValueCount(p) != 1) {
          if (rightValueBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (argValueBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (argValueBlock.getValueCount(p) != 1) {
          if (argValueBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBoolean(StDWithin.processCartesianPointDocValuesAndFieldAndField(leftValueBlock.getLong(leftValueBlock.getFirstValueIndex(p)), rightValueBlock.getBytesRef(rightValueBlock.getFirstValueIndex(p), rightValueScratch), argValueBlock.getDouble(argValueBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, LongVector leftValueVector,
      BytesRefVector rightValueVector, DoubleVector argValueVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      BytesRef rightValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(p, StDWithin.processCartesianPointDocValuesAndFieldAndField(leftValueVector.getLong(p), rightValueVector.getBytesRef(p, rightValueScratch), argValueVector.getDouble(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", argValue=" + argValue + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(leftValue, rightValue, argValue);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory leftValue;

    private final EvalOperator.ExpressionEvaluator.Factory rightValue;

    private final EvalOperator.ExpressionEvaluator.Factory argValue;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory leftValue,
        EvalOperator.ExpressionEvaluator.Factory rightValue,
        EvalOperator.ExpressionEvaluator.Factory argValue) {
      this.source = source;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
      this.argValue = argValue;
    }

    @Override
    public StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator get(DriverContext context) {
      return new StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator(source, leftValue.get(context), rightValue.get(context), argValue.get(context), context);
    }

    @Override
    public String toString() {
      return "StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", argValue=" + argValue + "]";
    }
  }
}
