// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
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
public final class StGeohashFromFieldDocValuesAndLiteralAndLiteralEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator encoded;

  private final StGeohash.GeoHashBoundedGrid bounds;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StGeohashFromFieldDocValuesAndLiteralAndLiteralEvaluator(Source source,
      EvalOperator.ExpressionEvaluator encoded, StGeohash.GeoHashBoundedGrid bounds,
      DriverContext driverContext) {
    this.source = source;
    this.encoded = encoded;
    this.bounds = bounds;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock encodedBlock = (LongBlock) encoded.eval(page)) {
      return eval(page.getPositionCount(), encodedBlock);
    }
  }

  public LongBlock eval(int positionCount, LongBlock encodedBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!encodedBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          StGeohash.fromFieldDocValuesAndLiteralAndLiteral(result, p, encodedBlock, this.bounds);
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
    return "StGeohashFromFieldDocValuesAndLiteralAndLiteralEvaluator[" + "encoded=" + encoded + ", bounds=" + bounds + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(encoded);
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

    private final EvalOperator.ExpressionEvaluator.Factory encoded;

    private final StGeohash.GeoHashBoundedGrid bounds;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory encoded,
        StGeohash.GeoHashBoundedGrid bounds) {
      this.source = source;
      this.encoded = encoded;
      this.bounds = bounds;
    }

    @Override
    public StGeohashFromFieldDocValuesAndLiteralAndLiteralEvaluator get(DriverContext context) {
      return new StGeohashFromFieldDocValuesAndLiteralAndLiteralEvaluator(source, encoded.get(context), bounds, context);
    }

    @Override
    public String toString() {
      return "StGeohashFromFieldDocValuesAndLiteralAndLiteralEvaluator[" + "encoded=" + encoded + ", bounds=" + bounds + "]";
    }
  }
}
