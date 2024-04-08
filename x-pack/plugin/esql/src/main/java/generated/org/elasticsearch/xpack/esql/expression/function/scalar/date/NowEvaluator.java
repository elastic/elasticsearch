// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Now}.
 * This class is generated. Do not edit it.
 */
public final class NowEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final long now;

  private final DriverContext driverContext;

  public NowEvaluator(Source source, long now, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.now = now;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    return eval(page.getPositionCount()).asBlock();
  }

  public LongVector eval(int positionCount) {
    try(LongVector.Builder result = driverContext.blockFactory().newLongVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendLong(Now.process(now));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "NowEvaluator[" + "now=" + now + "]";
  }

  @Override
  public void close() {
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final long now;

    public Factory(Source source, long now) {
      this.source = source;
      this.now = now;
    }

    @Override
    public NowEvaluator get(DriverContext context) {
      return new NowEvaluator(source, now, context);
    }

    @Override
    public String toString() {
      return "NowEvaluator[" + "now=" + now + "]";
    }
  }
}
