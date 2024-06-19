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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StDWithin}.
 * This class is generated. Do not edit it.
 */
public final class StDWithinGeoFieldAndFieldAndFieldEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final EvalOperator.ExpressionEvaluator rightValue;

  private final EvalOperator.ExpressionEvaluator distance;

  private final DriverContext driverContext;

  public StDWithinGeoFieldAndFieldAndFieldEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, EvalOperator.ExpressionEvaluator rightValue,
      EvalOperator.ExpressionEvaluator distance, DriverContext driverContext) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.distance = distance;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock leftValueBlock = (BytesRefBlock) leftValue.eval(page)) {
      try (BytesRefBlock rightValueBlock = (BytesRefBlock) rightValue.eval(page)) {
        try (DoubleBlock distanceBlock = (DoubleBlock) distance.eval(page)) {
          BytesRefVector leftValueVector = leftValueBlock.asVector();
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
          return eval(page.getPositionCount(), leftValueVector, rightValueVector, distanceVector);
        }
      }
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock leftValueBlock,
      BytesRefBlock rightValueBlock, DoubleBlock distanceBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef leftValueScratch = new BytesRef();
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
        try {
          result.appendBoolean(StDWithin.processGeoFieldAndFieldAndField(leftValueBlock.getBytesRef(leftValueBlock.getFirstValueIndex(p), leftValueScratch), rightValueBlock.getBytesRef(rightValueBlock.getFirstValueIndex(p), rightValueScratch), distanceBlock.getDouble(distanceBlock.getFirstValueIndex(p))));
        } catch (IllegalArgumentException | IOException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefVector leftValueVector,
      BytesRefVector rightValueVector, DoubleVector distanceVector) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef leftValueScratch = new BytesRef();
      BytesRef rightValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBoolean(StDWithin.processGeoFieldAndFieldAndField(leftValueVector.getBytesRef(p, leftValueScratch), rightValueVector.getBytesRef(p, rightValueScratch), distanceVector.getDouble(p)));
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
    return "StDWithinGeoFieldAndFieldAndFieldEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", distance=" + distance + "]";
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
    public StDWithinGeoFieldAndFieldAndFieldEvaluator get(DriverContext context) {
      return new StDWithinGeoFieldAndFieldAndFieldEvaluator(source, leftValue.get(context), rightValue.get(context), distance.get(context), context);
    }

    @Override
    public String toString() {
      return "StDWithinGeoFieldAndFieldAndFieldEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", distance=" + distance + "]";
    }
  }
}
