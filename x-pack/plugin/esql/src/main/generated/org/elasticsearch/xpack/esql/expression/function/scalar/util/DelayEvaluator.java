// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.util;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Delay}.
 * This class is generated. Do not edit it.
 */
public final class DelayEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final long ms;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DelayEvaluator(Source source, long ms, DriverContext driverContext) {
    this.source = source;
    this.ms = ms;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    return eval(page.getPositionCount()).asBlock();
  }

  public BooleanVector eval(int positionCount) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(p, Delay.process(this.ms));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DelayEvaluator[" + "ms=" + ms + "]";
  }

  @Override
  public void close() {
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

    private final long ms;

    public Factory(Source source, long ms) {
      this.source = source;
      this.ms = ms;
    }

    @Override
    public DelayEvaluator get(DriverContext context) {
      return new DelayEvaluator(source, ms, context);
    }

    @Override
    public String toString() {
      return "DelayEvaluator[" + "ms=" + ms + "]";
    }
  }
}
