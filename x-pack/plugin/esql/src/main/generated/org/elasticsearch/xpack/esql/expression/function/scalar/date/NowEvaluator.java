// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.EvaluatorKernel;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.FusableEvaluatorFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Now}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class NowEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(NowEvaluator.class);

  /**
   * The per-row function of this evaluator, callable through {@link #process}.
   */
  public static final EvaluatorKernel KERNEL = new EvaluatorKernel(NowEvaluator.class, long.class, List.of(EvaluatorKernel.fixed(long.class)), List.of());

  private final Source source;

  private final long now;

  private final DriverContext driverContext;

  private Warnings warnings;

  public NowEvaluator(Source source, long now, DriverContext driverContext) {
    this.source = source;
    this.now = now;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    return eval(page.getPositionCount()).asBlock();
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    return baseRamBytesUsed;
  }

  /**
   * The per-row function of this evaluator, described by {@link #KERNEL}.
   */
  public static long process(long now) {
    return Now.process(now);
  }

  public LongVector eval(int positionCount) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendLong(p, Now.process(this.now));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "NowEvaluator[" + "now=" + now + "]";
  }

  @Override
  public void close() {
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements FusableEvaluatorFactory {
    private final Source source;

    private final long now;

    public Factory(Source source, long now) {
      this.source = source;
      this.now = now;
    }

    @Override
    public NowEvaluator get(DriverContext context) {
      return new NowEvaluator(source, now, context);
    }

    @Override
    public String toString() {
      return "NowEvaluator[" + "now=" + now + "]";
    }

    @Override
    public EvaluatorKernel kernel() {
      return KERNEL;
    }

    @Override
    public List<ExpressionEvaluator.Factory> inputs() {
      return List.of();
    }

    @Override
    public List<Object> fixedValues() {
      return Arrays.<Object>asList(now);
    }

    @Override
    public Source source() {
      return source;
    }
  }
}
