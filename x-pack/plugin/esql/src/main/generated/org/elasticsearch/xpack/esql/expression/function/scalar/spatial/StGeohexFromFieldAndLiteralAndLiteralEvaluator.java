// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StGeohex}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StGeohexFromFieldAndLiteralAndLiteralEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator in;

  private final StGeohex.GeoHexBoundedGrid bounds;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StGeohexFromFieldAndLiteralAndLiteralEvaluator(Source source,
      EvalOperator.ExpressionEvaluator in, StGeohex.GeoHexBoundedGrid bounds,
      DriverContext driverContext) {
    this.source = source;
    this.in = in;
    this.bounds = bounds;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock inBlock = (BytesRefBlock) in.eval(page)) {
      return eval(page.getPositionCount(), inBlock);
    }
  }

  public LongBlock eval(int positionCount, BytesRefBlock inBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!inBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          StGeohex.fromFieldAndLiteralAndLiteral(result, p, inBlock, this.bounds);
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StGeohexFromFieldAndLiteralAndLiteralEvaluator[" + "in=" + in + ", bounds=" + bounds + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(in);
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

    private final EvalOperator.ExpressionEvaluator.Factory in;

    private final StGeohex.GeoHexBoundedGrid bounds;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory in,
        StGeohex.GeoHexBoundedGrid bounds) {
      this.source = source;
      this.in = in;
      this.bounds = bounds;
    }

    @Override
    public StGeohexFromFieldAndLiteralAndLiteralEvaluator get(DriverContext context) {
      return new StGeohexFromFieldAndLiteralAndLiteralEvaluator(source, in.get(context), bounds, context);
    }

    @Override
    public String toString() {
      return "StGeohexFromFieldAndLiteralAndLiteralEvaluator[" + "in=" + in + ", bounds=" + bounds + "]";
    }
  }
}
