// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.ArithmeticException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Log}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class LogEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LogEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator base;

  private final EvalOperator.ExpressionEvaluator value;

  private final DriverContext driverContext;

  private Warnings warnings;

  public LogEvaluator(Source source, EvalOperator.ExpressionEvaluator base,
      EvalOperator.ExpressionEvaluator value, DriverContext driverContext) {
    this.source = source;
    this.base = base;
    this.value = value;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock baseBlock = (DoubleBlock) base.eval(page)) {
      try (DoubleBlock valueBlock = (DoubleBlock) value.eval(page)) {
        DoubleVector baseVector = baseBlock.asVector();
        if (baseVector == null) {
          return eval(page.getPositionCount(), baseBlock, valueBlock);
        }
        DoubleVector valueVector = valueBlock.asVector();
        if (valueVector == null) {
          return eval(page.getPositionCount(), baseBlock, valueBlock);
        }
        return eval(page.getPositionCount(), baseVector, valueVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += base.baseRamBytesUsed();
    baseRamBytesUsed += value.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, DoubleBlock baseBlock, DoubleBlock valueBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (baseBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        switch (valueBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        double base = baseBlock.getDouble(baseBlock.getFirstValueIndex(p));
        double value = valueBlock.getDouble(valueBlock.getFirstValueIndex(p));
        try {
          result.appendDouble(Log.process(base, value));
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, DoubleVector baseVector, DoubleVector valueVector) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        double base = baseVector.getDouble(p);
        double value = valueVector.getDouble(p);
        try {
          result.appendDouble(Log.process(base, value));
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "LogEvaluator[" + "base=" + base + ", value=" + value + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(base, value);
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

    private final EvalOperator.ExpressionEvaluator.Factory base;

    private final EvalOperator.ExpressionEvaluator.Factory value;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory base,
        EvalOperator.ExpressionEvaluator.Factory value) {
      this.source = source;
      this.base = base;
      this.value = value;
    }

    @Override
    public LogEvaluator get(DriverContext context) {
      return new LogEvaluator(source, base.get(context), value.get(context), context);
    }

    @Override
    public String toString() {
      return "LogEvaluator[" + "base=" + base + ", value=" + value + "]";
    }
  }
}
