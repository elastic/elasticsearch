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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
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
public final class StGeotileFromFieldDocValuesAndFieldEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator encoded;

  private final EvalOperator.ExpressionEvaluator precision;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StGeotileFromFieldDocValuesAndFieldEvaluator(Source source,
      EvalOperator.ExpressionEvaluator encoded, EvalOperator.ExpressionEvaluator precision,
      DriverContext driverContext) {
    this.source = source;
    this.encoded = encoded;
    this.precision = precision;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock encodedBlock = (LongBlock) encoded.eval(page)) {
      try (IntBlock precisionBlock = (IntBlock) precision.eval(page)) {
        return eval(page.getPositionCount(), encodedBlock, precisionBlock);
      }
    }
  }

  public BytesRefBlock eval(int positionCount, LongBlock encodedBlock, IntBlock precisionBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!encodedBlock.isNull(p)) {
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
          StGeotile.fromFieldDocValuesAndField(result, p, encodedBlock, precisionBlock.getInt(precisionBlock.getFirstValueIndex(p)));
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
    return "StGeotileFromFieldDocValuesAndFieldEvaluator[" + "encoded=" + encoded + ", precision=" + precision + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(encoded, precision);
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

    private final EvalOperator.ExpressionEvaluator.Factory precision;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory encoded,
        EvalOperator.ExpressionEvaluator.Factory precision) {
      this.source = source;
      this.encoded = encoded;
      this.precision = precision;
    }

    @Override
    public StGeotileFromFieldDocValuesAndFieldEvaluator get(DriverContext context) {
      return new StGeotileFromFieldDocValuesAndFieldEvaluator(source, encoded.get(context), precision.get(context), context);
    }

    @Override
    public String toString() {
      return "StGeotileFromFieldDocValuesAndFieldEvaluator[" + "encoded=" + encoded + ", precision=" + precision + "]";
    }
  }
}
