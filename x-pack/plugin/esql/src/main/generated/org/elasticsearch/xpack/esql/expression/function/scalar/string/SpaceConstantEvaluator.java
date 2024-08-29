// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Space}.
 * This class is generated. Do not edit it.
 */
public final class SpaceConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final BreakingBytesRefBuilder scratch;

  private final int number;

  private final DriverContext driverContext;

  public SpaceConstantEvaluator(Source source, BreakingBytesRefBuilder scratch, int number,
      DriverContext driverContext) {
    this.scratch = scratch;
    this.number = number;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    return eval(page.getPositionCount()).asBlock();
  }

  public BytesRefVector eval(int positionCount) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(Space.processConstant(this.scratch, this.number));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "SpaceConstantEvaluator[" + "number=" + number + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(scratch);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final Function<DriverContext, BreakingBytesRefBuilder> scratch;

    private final int number;

    public Factory(Source source, Function<DriverContext, BreakingBytesRefBuilder> scratch,
        int number) {
      this.source = source;
      this.scratch = scratch;
      this.number = number;
    }

    @Override
    public SpaceConstantEvaluator get(DriverContext context) {
      return new SpaceConstantEvaluator(source, scratch.apply(context), number, context);
    }

    @Override
    public String toString() {
      return "SpaceConstantEvaluator[" + "number=" + number + "]";
    }
  }
}
