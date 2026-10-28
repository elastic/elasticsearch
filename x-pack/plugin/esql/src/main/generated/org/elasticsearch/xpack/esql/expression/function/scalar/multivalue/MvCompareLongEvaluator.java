// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link MvCompare}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class MvCompareLongEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvCompareLongEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator field;

  private final ExpressionEvaluator bound;

  private final boolean greater;

  private final boolean includeBound;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvCompareLongEvaluator(Source source, ExpressionEvaluator field, ExpressionEvaluator bound,
      boolean greater, boolean includeBound, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.bound = bound;
    this.greater = greater;
    this.includeBound = includeBound;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock fieldBlock = (LongBlock) field.eval(page)) {
      try (LongBlock boundBlock = (LongBlock) bound.eval(page)) {
        return eval(page.getPositionCount(), fieldBlock, boundBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    baseRamBytesUsed += bound.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, LongBlock fieldBlock, LongBlock boundBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(MvCompare.process(p, fieldBlock, boundBlock, this.greater, this.includeBound));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvCompareLongEvaluator[" + "field=" + field + ", bound=" + bound + ", greater=" + greater + ", includeBound=" + includeBound + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field, bound);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory field;

    private final ExpressionEvaluator.Factory bound;

    private final boolean greater;

    private final boolean includeBound;

    public Factory(Source source, ExpressionEvaluator.Factory field,
        ExpressionEvaluator.Factory bound, boolean greater, boolean includeBound) {
      this.source = source;
      this.field = field;
      this.bound = bound;
      this.greater = greater;
      this.includeBound = includeBound;
    }

    @Override
    public MvCompareLongEvaluator get(DriverContext context) {
      return new MvCompareLongEvaluator(source, field.get(context), bound.get(context), greater, includeBound, context);
    }

    @Override
    public String toString() {
      return "MvCompareLongEvaluator[" + "field=" + field + ", bound=" + bound + ", greater=" + greater + ", includeBound=" + includeBound + "]";
    }
  }
}
