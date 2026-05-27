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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link RoundToInt}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RoundToIntFloorSearchEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RoundToIntFloorSearchEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator v;

  private final int[] p;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RoundToIntFloorSearchEvaluator(Source source, ExpressionEvaluator v, int[] p,
      DriverContext driverContext) {
    this.source = source;
    this.v = v;
    this.p = p;
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

  public IntBlock eval(int positionCount, IntBlock vBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
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
        result.appendInt(RoundToInt.floorSearch(v, this.p));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, IntVector vVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        int v = vVector.getInt(p);
        result.appendInt(p, RoundToInt.floorSearch(v, this.p));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RoundToIntFloorSearchEvaluator[" + "v=" + v + "]";
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

    private final int[] p;

    public Factory(Source source, ExpressionEvaluator.Factory v, int[] p) {
      this.source = source;
      this.v = v;
      this.p = p;
    }

    @Override
    public RoundToIntFloorSearchEvaluator get(DriverContext context) {
      return new RoundToIntFloorSearchEvaluator(source, v.get(context), p, context);
    }

    @Override
    public String toString() {
      return "RoundToIntFloorSearchEvaluator[" + "v=" + v + "]";
    }
  }
}
