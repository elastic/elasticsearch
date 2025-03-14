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
import org.elasticsearch.compute.data.LongVector;
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
public final class StGeohashFromFieldDocValuesAndLiteralEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator encoded;

  private final int precision;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StGeohashFromFieldDocValuesAndLiteralEvaluator(Source source,
      EvalOperator.ExpressionEvaluator encoded, int precision, DriverContext driverContext) {
    this.source = source;
    this.encoded = encoded;
    this.precision = precision;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock encodedBlock = (LongBlock) encoded.eval(page)) {
      LongVector encodedVector = encodedBlock.asVector();
      if (encodedVector == null) {
        return eval(page.getPositionCount(), encodedBlock);
      }
      return eval(page.getPositionCount(), encodedVector);
    }
  }

  public BytesRefBlock eval(int positionCount, LongBlock encodedBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (encodedBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (encodedBlock.getValueCount(p) != 1) {
          if (encodedBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendBytesRef(StGeohash.fromFieldDocValuesAndLiteral(encodedBlock.getLong(encodedBlock.getFirstValueIndex(p)), this.precision));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, LongVector encodedVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBytesRef(StGeohash.fromFieldDocValuesAndLiteral(encodedVector.getLong(p), this.precision));
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
    return "StGeohashFromFieldDocValuesAndLiteralEvaluator[" + "encoded=" + encoded + ", precision=" + precision + "]";
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

    private final int precision;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory encoded, int precision) {
      this.source = source;
      this.encoded = encoded;
      this.precision = precision;
    }

    @Override
    public StGeohashFromFieldDocValuesAndLiteralEvaluator get(DriverContext context) {
      return new StGeohashFromFieldDocValuesAndLiteralEvaluator(source, encoded.get(context), precision, context);
    }

    @Override
    public String toString() {
      return "StGeohashFromFieldDocValuesAndLiteralEvaluator[" + "encoded=" + encoded + ", precision=" + precision + "]";
    }
  }
}
