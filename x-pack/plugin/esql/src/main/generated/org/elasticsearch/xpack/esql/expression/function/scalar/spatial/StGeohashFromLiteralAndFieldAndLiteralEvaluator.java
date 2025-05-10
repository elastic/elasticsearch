// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
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
public final class StGeohashFromLiteralAndFieldAndLiteralEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final BytesRef in;

  private final EvalOperator.ExpressionEvaluator precision;

  private final GeoBoundingBox bbox;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StGeohashFromLiteralAndFieldAndLiteralEvaluator(Source source, BytesRef in,
      EvalOperator.ExpressionEvaluator precision, GeoBoundingBox bbox,
      DriverContext driverContext) {
    this.source = source;
    this.in = in;
    this.precision = precision;
    this.bbox = bbox;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock precisionBlock = (IntBlock) precision.eval(page)) {
      IntVector precisionVector = precisionBlock.asVector();
      if (precisionVector == null) {
        return eval(page.getPositionCount(), precisionBlock);
      }
      return eval(page.getPositionCount(), precisionVector);
    }
  }

  public LongBlock eval(int positionCount, IntBlock precisionBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
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
        try {
          result.appendLong(StGeohash.fromLiteralAndFieldAndLiteral(this.in, precisionBlock.getInt(precisionBlock.getFirstValueIndex(p)), this.bbox));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public LongBlock eval(int positionCount, IntVector precisionVector) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendLong(StGeohash.fromLiteralAndFieldAndLiteral(this.in, precisionVector.getInt(p), this.bbox));
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
    return "StGeohashFromLiteralAndFieldAndLiteralEvaluator[" + "in=" + in + ", precision=" + precision + ", bbox=" + bbox + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(precision);
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

    private final BytesRef in;

    private final EvalOperator.ExpressionEvaluator.Factory precision;

    private final GeoBoundingBox bbox;

    public Factory(Source source, BytesRef in, EvalOperator.ExpressionEvaluator.Factory precision,
        GeoBoundingBox bbox) {
      this.source = source;
      this.in = in;
      this.precision = precision;
      this.bbox = bbox;
    }

    @Override
    public StGeohashFromLiteralAndFieldAndLiteralEvaluator get(DriverContext context) {
      return new StGeohashFromLiteralAndFieldAndLiteralEvaluator(source, in, precision.get(context), bbox, context);
    }

    @Override
    public String toString() {
      return "StGeohashFromLiteralAndFieldAndLiteralEvaluator[" + "in=" + in + ", precision=" + precision + ", bbox=" + bbox + "]";
    }
  }
}
