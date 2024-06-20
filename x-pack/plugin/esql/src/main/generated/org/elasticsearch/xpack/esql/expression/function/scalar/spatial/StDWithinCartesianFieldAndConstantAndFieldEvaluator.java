// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StDWithin}.
 * This class is generated. Do not edit it.
 */
public final class StDWithinCartesianFieldAndConstantAndFieldEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final Point rightValue;

  private final EvalOperator.ExpressionEvaluator argValue;

  private final DriverContext driverContext;

  public StDWithinCartesianFieldAndConstantAndFieldEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, Point rightValue,
      EvalOperator.ExpressionEvaluator argValue, DriverContext driverContext) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.argValue = argValue;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock leftValueBlock = (BytesRefBlock) leftValue.eval(page)) {
      try (DoubleBlock argValueBlock = (DoubleBlock) argValue.eval(page)) {
        BytesRefVector leftValueVector = leftValueBlock.asVector();
        if (leftValueVector == null) {
          return eval(page.getPositionCount(), leftValueBlock, argValueBlock);
        }
        DoubleVector argValueVector = argValueBlock.asVector();
        if (argValueVector == null) {
          return eval(page.getPositionCount(), leftValueBlock, argValueBlock);
        }
        return eval(page.getPositionCount(), leftValueVector, argValueVector);
      }
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock leftValueBlock,
      DoubleBlock argValueBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef leftValueScratch = new BytesRef();
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
        try {
          result.appendBoolean(StDWithin.processCartesianFieldAndConstantAndField(leftValueBlock.getBytesRef(leftValueBlock.getFirstValueIndex(p), leftValueScratch), rightValue, argValueBlock.getDouble(argValueBlock.getFirstValueIndex(p))));
        } catch (IllegalArgumentException | IOException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefVector leftValueVector,
      DoubleVector argValueVector) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef leftValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBoolean(StDWithin.processCartesianFieldAndConstantAndField(leftValueVector.getBytesRef(p, leftValueScratch), rightValue, argValueVector.getDouble(p)));
        } catch (IllegalArgumentException | IOException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StDWithinCartesianFieldAndConstantAndFieldEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", argValue=" + argValue + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(leftValue, argValue);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory leftValue;

    private final Point rightValue;

    private final EvalOperator.ExpressionEvaluator.Factory argValue;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory leftValue,
        Point rightValue, EvalOperator.ExpressionEvaluator.Factory argValue) {
      this.source = source;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
      this.argValue = argValue;
    }

    @Override
    public StDWithinCartesianFieldAndConstantAndFieldEvaluator get(DriverContext context) {
      return new StDWithinCartesianFieldAndConstantAndFieldEvaluator(source, leftValue.get(context), rightValue, argValue.get(context), context);
    }

    @Override
    public String toString() {
      return "StDWithinCartesianFieldAndConstantAndFieldEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", argValue=" + argValue + "]";
    }
  }
}
