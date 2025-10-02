// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link CopySign}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class CopySignFloatEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CopySignFloatEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator magnitude;

  private final EvalOperator.ExpressionEvaluator sign;

  private final DriverContext driverContext;

  private Warnings warnings;

  public CopySignFloatEvaluator(Source source, EvalOperator.ExpressionEvaluator magnitude,
      EvalOperator.ExpressionEvaluator sign, DriverContext driverContext) {
    this.source = source;
    this.magnitude = magnitude;
    this.sign = sign;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (FloatBlock magnitudeBlock = (FloatBlock) magnitude.eval(page)) {
      try (DoubleBlock signBlock = (DoubleBlock) sign.eval(page)) {
        FloatVector magnitudeVector = magnitudeBlock.asVector();
        if (magnitudeVector == null) {
          return eval(page.getPositionCount(), magnitudeBlock, signBlock);
        }
        DoubleVector signVector = signBlock.asVector();
        if (signVector == null) {
          return eval(page.getPositionCount(), magnitudeBlock, signBlock);
        }
        return eval(page.getPositionCount(), magnitudeVector, signVector).asBlock();
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += magnitude.baseRamBytesUsed();
    baseRamBytesUsed += sign.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public FloatBlock eval(int positionCount, FloatBlock magnitudeBlock, DoubleBlock signBlock) {
    try(FloatBlock.Builder result = driverContext.blockFactory().newFloatBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (magnitudeBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (magnitudeBlock.getValueCount(p) != 1) {
          if (magnitudeBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (signBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (signBlock.getValueCount(p) != 1) {
          if (signBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        float magnitude = magnitudeBlock.getFloat(magnitudeBlock.getFirstValueIndex(p));
        double sign = signBlock.getDouble(signBlock.getFirstValueIndex(p));
        result.appendFloat(CopySign.processFloat(magnitude, sign));
      }
      return result.build();
    }
  }

  public FloatVector eval(int positionCount, FloatVector magnitudeVector, DoubleVector signVector) {
    try(FloatVector.FixedBuilder result = driverContext.blockFactory().newFloatVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        float magnitude = magnitudeVector.getFloat(p);
        double sign = signVector.getDouble(p);
        result.appendFloat(p, CopySign.processFloat(magnitude, sign));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "CopySignFloatEvaluator[" + "magnitude=" + magnitude + ", sign=" + sign + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(magnitude, sign);
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

    private final EvalOperator.ExpressionEvaluator.Factory magnitude;

    private final EvalOperator.ExpressionEvaluator.Factory sign;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory magnitude,
        EvalOperator.ExpressionEvaluator.Factory sign) {
      this.source = source;
      this.magnitude = magnitude;
      this.sign = sign;
    }

    @Override
    public CopySignFloatEvaluator get(DriverContext context) {
      return new CopySignFloatEvaluator(source, magnitude.get(context), sign.get(context), context);
    }

    @Override
    public String toString() {
      return "CopySignFloatEvaluator[" + "magnitude=" + magnitude + ", sign=" + sign + "]";
    }
  }
}
