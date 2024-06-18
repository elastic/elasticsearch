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
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StDWithin}.
 * This class is generated. Do not edit it.
 */
public final class StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final EvalOperator.ExpressionEvaluator rightValue;

  private final EvalOperator.ExpressionEvaluator distance;

  private final DriverContext driverContext;

  public StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, EvalOperator.ExpressionEvaluator rightValue,
      EvalOperator.ExpressionEvaluator distance, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.distance = distance;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock leftValueBlock = (LongBlock) leftValue.eval(page)) {
      try (BytesRefBlock rightValueBlock = (BytesRefBlock) rightValue.eval(page)) {
        try (DoubleBlock distanceBlock = (DoubleBlock) distance.eval(page)) {
          LongVector leftValueVector = leftValueBlock.asVector();
          if (leftValueVector == null) {
            return eval(page.getPositionCount(), leftValueBlock, rightValueBlock, distanceBlock);
          }
          BytesRefVector rightValueVector = rightValueBlock.asVector();
          if (rightValueVector == null) {
            return eval(page.getPositionCount(), leftValueBlock, rightValueBlock, distanceBlock);
          }
          DoubleVector distanceVector = distanceBlock.asVector();
          if (distanceVector == null) {
            return eval(page.getPositionCount(), leftValueBlock, rightValueBlock, distanceBlock);
          }
          return eval(page.getPositionCount(), leftValueVector, rightValueVector, distanceVector).asBlock();
        }
      }
    }
  }

  public BooleanBlock eval(int positionCount, LongBlock leftValueBlock,
      BytesRefBlock rightValueBlock, DoubleBlock distanceBlock) {
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
        if (distanceBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (distanceBlock.getValueCount(p) != 1) {
          if (distanceBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBoolean(StDWithin.processCartesianPointDocValuesAndFieldAndField(leftValueBlock.getLong(leftValueBlock.getFirstValueIndex(p)), rightValueBlock.getBytesRef(rightValueBlock.getFirstValueIndex(p), rightValueScratch), distanceBlock.getDouble(distanceBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, LongVector leftValueVector,
      BytesRefVector rightValueVector, DoubleVector distanceVector) {
    try(BooleanVector.Builder result = driverContext.blockFactory().newBooleanVectorBuilder(positionCount)) {
      BytesRef rightValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(StDWithin.processCartesianPointDocValuesAndFieldAndField(leftValueVector.getLong(p), rightValueVector.getBytesRef(p, rightValueScratch), distanceVector.getDouble(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", distance=" + distance + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(leftValue, rightValue, distance);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory leftValue;

    private final EvalOperator.ExpressionEvaluator.Factory rightValue;

    private final EvalOperator.ExpressionEvaluator.Factory distance;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory leftValue,
        EvalOperator.ExpressionEvaluator.Factory rightValue,
        EvalOperator.ExpressionEvaluator.Factory distance) {
      this.source = source;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
      this.distance = distance;
    }

    @Override
    public StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator get(DriverContext context) {
      return new StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator(source, leftValue.get(context), rightValue.get(context), distance.get(context), context);
    }

    @Override
    public String toString() {
      return "StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", distance=" + distance + "]";
    }
  }
}
