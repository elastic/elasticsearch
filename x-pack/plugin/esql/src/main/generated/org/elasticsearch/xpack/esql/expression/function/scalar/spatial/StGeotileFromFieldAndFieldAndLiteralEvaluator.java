// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StGeotile}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StGeotileFromFieldAndFieldAndLiteralEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator in;

  private final EvalOperator.ExpressionEvaluator precision;

  private final GeoBoundingBox bbox;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StGeotileFromFieldAndFieldAndLiteralEvaluator(Source source,
      EvalOperator.ExpressionEvaluator in, EvalOperator.ExpressionEvaluator precision,
      GeoBoundingBox bbox, DriverContext driverContext) {
    this.source = source;
    this.in = in;
    this.precision = precision;
    this.bbox = bbox;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock inBlock = (BytesRefBlock) in.eval(page)) {
      try (IntBlock precisionBlock = (IntBlock) precision.eval(page)) {
        return eval(page.getPositionCount(), inBlock, precisionBlock);
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock inBlock, IntBlock precisionBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!inBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (precisionBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (precisionBlock.getValueCount(p) != 1) {
          if (precisionBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          StGeotile.fromFieldAndFieldAndLiteral(result, p, inBlock, precisionBlock.getInt(precisionBlock.getFirstValueIndex(p)), this.bbox);
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
    return "StGeotileFromFieldAndFieldAndLiteralEvaluator[" + "in=" + in + ", precision=" + precision + ", bbox=" + bbox + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(in, precision);
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

    private final EvalOperator.ExpressionEvaluator.Factory precision;

    private final GeoBoundingBox bbox;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory in,
        EvalOperator.ExpressionEvaluator.Factory precision, GeoBoundingBox bbox) {
      this.source = source;
      this.in = in;
      this.precision = precision;
      this.bbox = bbox;
    }

    @Override
    public StGeotileFromFieldAndFieldAndLiteralEvaluator get(DriverContext context) {
      return new StGeotileFromFieldAndFieldAndLiteralEvaluator(source, in.get(context), precision.get(context), bbox, context);
    }

    @Override
    public String toString() {
      return "StGeotileFromFieldAndFieldAndLiteralEvaluator[" + "in=" + in + ", precision=" + precision + ", bbox=" + bbox + "]";
    }
  }
}
