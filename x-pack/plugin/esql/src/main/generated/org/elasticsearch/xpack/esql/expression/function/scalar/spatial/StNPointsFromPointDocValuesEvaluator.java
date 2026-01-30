// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StNPoints}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StNPointsFromPointDocValuesEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StNPointsFromPointDocValuesEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator encoded;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StNPointsFromPointDocValuesEvaluator(Source source,
      EvalOperator.ExpressionEvaluator encoded, DriverContext driverContext) {
    this.source = source;
    this.encoded = encoded;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock encodedBlock = (LongBlock) encoded.eval(page)) {
      return eval(page.getPositionCount(), encodedBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += encoded.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, LongBlock encodedBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
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
          StNPoints.fromPointDocValues(result, p, encodedBlock);
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
    return "StNPointsFromPointDocValuesEvaluator[" + "encoded=" + encoded + "]";
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

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory encoded) {
      this.source = source;
      this.encoded = encoded;
    }

    @Override
    public StNPointsFromPointDocValuesEvaluator get(DriverContext context) {
      return new StNPointsFromPointDocValuesEvaluator(source, encoded.get(context), context);
    }

    @Override
    public String toString() {
      return "StNPointsFromPointDocValuesEvaluator[" + "encoded=" + encoded + "]";
    }
  }
}
