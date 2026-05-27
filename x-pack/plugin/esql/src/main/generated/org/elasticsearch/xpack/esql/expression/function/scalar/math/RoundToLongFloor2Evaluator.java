// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link RoundToLong}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RoundToLongFloor2Evaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RoundToLongFloor2Evaluator.class);

  private final Source source;

  private final ExpressionEvaluator v;

  private final long p0;

  private final long p1;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RoundToLongFloor2Evaluator(Source source, ExpressionEvaluator v, long p0, long p1,
      DriverContext driverContext) {
    this.source = source;
    this.v = v;
    this.p0 = p0;
    this.p1 = p1;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock vBlock = (LongBlock) v.eval(page)) {
      LongVector vVector = vBlock.asVector();
      if (vVector == null) {
        return eval(page.getPositionCount(), vBlock);
      }
      return eval(page.getPositionCount(), vVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += v.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, LongBlock vBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (vBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        long v = vBlock.getLong(vBlock.getFirstValueIndex(p));
        result.appendLong(RoundToLong.floor2(v, this.p0, this.p1));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector vVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long v = vVector.getLong(p);
        result.appendLong(p, RoundToLong.floor2(v, this.p0, this.p1));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RoundToLongFloor2Evaluator[" + "v=" + v + ", p0=" + p0 + ", p1=" + p1 + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(v);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory v;

    private final long p0;

    private final long p1;

    public Factory(Source source, ExpressionEvaluator.Factory v, long p0, long p1) {
      this.source = source;
      this.v = v;
      this.p0 = p0;
      this.p1 = p1;
    }

    @Override
    public RoundToLongFloor2Evaluator get(DriverContext context) {
      return new RoundToLongFloor2Evaluator(source, v.get(context), p0, p1, context);
    }

    @Override
    public String toString() {
      return "RoundToLongFloor2Evaluator[" + "v=" + v + ", p0=" + p0 + ", p1=" + p1 + "]";
    }
  }
}
