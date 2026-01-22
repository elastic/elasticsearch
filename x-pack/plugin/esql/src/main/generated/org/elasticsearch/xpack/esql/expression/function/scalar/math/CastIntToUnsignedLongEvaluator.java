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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Cast}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class CastIntToUnsignedLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CastIntToUnsignedLongEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator v;

  private final DriverContext driverContext;

  private Warnings warnings;

  public CastIntToUnsignedLongEvaluator(Source source, EvalOperator.ExpressionEvaluator v,
      DriverContext driverContext) {
    this.source = source;
    this.v = v;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock vBlock = (IntBlock) v.eval(page)) {
      IntVector vVector = vBlock.asVector();
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

  public LongBlock eval(int positionCount, IntBlock vBlock) {
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
        int v = vBlock.getInt(vBlock.getFirstValueIndex(p));
        result.appendLong(Cast.castIntToUnsignedLong(v));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, IntVector vVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        int v = vVector.getInt(p);
        result.appendLong(p, Cast.castIntToUnsignedLong(v));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "CastIntToUnsignedLongEvaluator[" + "v=" + v + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(v);
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

    private final EvalOperator.ExpressionEvaluator.Factory v;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory v) {
      this.source = source;
      this.v = v;
    }

    @Override
    public CastIntToUnsignedLongEvaluator get(DriverContext context) {
      return new CastIntToUnsignedLongEvaluator(source, v.get(context), context);
    }

    @Override
    public String toString() {
      return "CastIntToUnsignedLongEvaluator[" + "v=" + v + "]";
    }
  }
}
