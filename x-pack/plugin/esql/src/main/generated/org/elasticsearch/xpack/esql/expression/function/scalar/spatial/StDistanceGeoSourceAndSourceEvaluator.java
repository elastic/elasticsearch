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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StDistance}.
 * This class is generated. Do not edit it.
 */
public final class StDistanceGeoSourceAndSourceEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final EvalOperator.ExpressionEvaluator rightValue;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StDistanceGeoSourceAndSourceEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, EvalOperator.ExpressionEvaluator rightValue,
      DriverContext driverContext) {
    this.source = source;
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock leftValueBlock = (BytesRefBlock) leftValue.eval(page)) {
      try (BytesRefBlock rightValueBlock = (BytesRefBlock) rightValue.eval(page)) {
        BytesRefVector leftValueVector = leftValueBlock.asVector();
        if (leftValueVector == null) {
          return eval(page.getPositionCount(), leftValueBlock, rightValueBlock);
        }
        BytesRefVector rightValueVector = rightValueBlock.asVector();
        if (rightValueVector == null) {
          return eval(page.getPositionCount(), leftValueBlock, rightValueBlock);
        }
        return eval(page.getPositionCount(), leftValueVector, rightValueVector);
      }
    }
  }

  public DoubleBlock eval(int positionCount, BytesRefBlock leftValueBlock,
      BytesRefBlock rightValueBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      BytesRef leftValueScratch = new BytesRef();
      BytesRef rightValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (leftValueBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (leftValueBlock.getValueCount(p) != 1) {
          if (leftValueBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
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
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendDouble(StDistance.processGeoSourceAndSource(leftValueBlock.getBytesRef(leftValueBlock.getFirstValueIndex(p), leftValueScratch), rightValueBlock.getBytesRef(rightValueBlock.getFirstValueIndex(p), rightValueScratch)));
        } catch (IllegalArgumentException | IOException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, BytesRefVector leftValueVector,
      BytesRefVector rightValueVector) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      BytesRef leftValueScratch = new BytesRef();
      BytesRef rightValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendDouble(StDistance.processGeoSourceAndSource(leftValueVector.getBytesRef(p, leftValueScratch), rightValueVector.getBytesRef(p, rightValueScratch)));
        } catch (IllegalArgumentException | IOException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StDistanceGeoSourceAndSourceEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(leftValue, rightValue);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(
              driverContext.warningsMode(),
              source.source().getLineNumber(),
              source.source().getColumnNumber(),
              source.text()
          );
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory leftValue;

    private final EvalOperator.ExpressionEvaluator.Factory rightValue;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory leftValue,
        EvalOperator.ExpressionEvaluator.Factory rightValue) {
      this.source = source;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
    }

    @Override
    public StDistanceGeoSourceAndSourceEvaluator get(DriverContext context) {
      return new StDistanceGeoSourceAndSourceEvaluator(source, leftValue.get(context), rightValue.get(context), context);
    }

    @Override
    public String toString() {
      return "StDistanceGeoSourceAndSourceEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
    }
  }
}
