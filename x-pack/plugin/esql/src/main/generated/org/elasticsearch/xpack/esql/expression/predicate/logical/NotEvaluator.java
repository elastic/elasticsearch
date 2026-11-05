// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.logical;

import java.lang.IllegalArgumentException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.EvaluatorKernel;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.FusableEvaluatorFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Not}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class NotEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(NotEvaluator.class);

  /**
   * The per-row function of this evaluator, callable through {@link #process}.
   */
  public static final EvaluatorKernel KERNEL = new EvaluatorKernel(NotEvaluator.class, boolean.class, List.of(EvaluatorKernel.value(boolean.class)), List.of());

  private final Source source;

  private final ExpressionEvaluator v;

  private final DriverContext driverContext;

  private Warnings warnings;

  public NotEvaluator(Source source, ExpressionEvaluator v, DriverContext driverContext) {
    this.source = source;
    this.v = v;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BooleanBlock vBlock = (BooleanBlock) v.eval(page)) {
      BooleanVector vVector = vBlock.asVector();
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

  /**
   * The per-row function of this evaluator, described by {@link #KERNEL}.
   */
  public static boolean process(boolean v) {
    return Not.process(v);
  }

  public BooleanBlock eval(int positionCount, BooleanBlock vBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (vBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (vBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        boolean v = vBlock.getBoolean(vBlock.getFirstValueIndex(p));
        result.appendBoolean(Not.process(v));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BooleanVector vVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean v = vVector.getBoolean(p);
        result.appendBoolean(p, Not.process(v));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "NotEvaluator[" + "v=" + v + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(v);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements FusableEvaluatorFactory {
    private final Source source;

    private final ExpressionEvaluator.Factory v;

    public Factory(Source source, ExpressionEvaluator.Factory v) {
      this.source = source;
      this.v = v;
    }

    @Override
    public NotEvaluator get(DriverContext context) {
      return new NotEvaluator(source, v.get(context), context);
    }

    @Override
    public String toString() {
      return "NotEvaluator[" + "v=" + v + "]";
    }

    @Override
    public EvaluatorKernel kernel() {
      return KERNEL;
    }

    @Override
    public List<ExpressionEvaluator.Factory> inputs() {
      return List.of(v);
    }

    @Override
    public List<Object> fixedValues() {
      return List.of();
    }

    @Override
    public Source source() {
      return source;
    }
  }
}
