// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.util;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Slow}.
 * This class is generated. Do not edit it.
 */
public final class SlowEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator condition;

  private final long ms;

  private final DriverContext driverContext;

  private Warnings warnings;

  public SlowEvaluator(Source source, EvalOperator.ExpressionEvaluator condition, long ms,
      DriverContext driverContext) {
    this.source = source;
    this.condition = condition;
    this.ms = ms;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BooleanBlock conditionBlock = (BooleanBlock) condition.eval(page)) {
      BooleanVector conditionVector = conditionBlock.asVector();
      if (conditionVector == null) {
        return eval(page.getPositionCount(), conditionBlock);
      }
      return eval(page.getPositionCount(), conditionVector).asBlock();
    }
  }

  public BooleanBlock eval(int positionCount, BooleanBlock conditionBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (conditionBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (conditionBlock.getValueCount(p) != 1) {
          if (conditionBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBoolean(Slow.process(conditionBlock.getBoolean(conditionBlock.getFirstValueIndex(p)), this.ms));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BooleanVector conditionVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(p, Slow.process(conditionVector.getBoolean(p), this.ms));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "SlowEvaluator[" + "condition=" + condition + ", ms=" + ms + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(condition);
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

    private final EvalOperator.ExpressionEvaluator.Factory condition;

    private final long ms;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory condition, long ms) {
      this.source = source;
      this.condition = condition;
      this.ms = ms;
    }

    @Override
    public SlowEvaluator get(DriverContext context) {
      return new SlowEvaluator(source, condition.get(context), ms, context);
    }

    @Override
    public String toString() {
      return "SlowEvaluator[" + "condition=" + condition + ", ms=" + ms + "]";
    }
  }
}
