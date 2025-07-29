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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StGeohash}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StGeohashFromFieldAndLiteralEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator wkbBlock;

  private final int precision;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StGeohashFromFieldAndLiteralEvaluator(Source source,
      EvalOperator.ExpressionEvaluator wkbBlock, int precision, DriverContext driverContext) {
    this.source = source;
    this.wkbBlock = wkbBlock;
    this.precision = precision;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock wkbBlockBlock = (BytesRefBlock) wkbBlock.eval(page)) {
      return eval(page.getPositionCount(), wkbBlockBlock);
    }
  }

  public LongBlock eval(int positionCount, BytesRefBlock wkbBlockBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!wkbBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          StGeohash.fromFieldAndLiteral(result, p, wkbBlockBlock, this.precision);
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
    return "StGeohashFromFieldAndLiteralEvaluator[" + "wkbBlock=" + wkbBlock + ", precision=" + precision + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(wkbBlock);
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

    private final EvalOperator.ExpressionEvaluator.Factory wkbBlock;

    private final int precision;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory wkbBlock,
        int precision) {
      this.source = source;
      this.wkbBlock = wkbBlock;
      this.precision = precision;
    }

    @Override
    public StGeohashFromFieldAndLiteralEvaluator get(DriverContext context) {
      return new StGeohashFromFieldAndLiteralEvaluator(source, wkbBlock.get(context), precision, context);
    }

    @Override
    public String toString() {
      return "StGeohashFromFieldAndLiteralEvaluator[" + "wkbBlock=" + wkbBlock + ", precision=" + precision + "]";
    }
  }
}
