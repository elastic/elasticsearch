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
 * {@link ExpressionEvaluator} implementation for {@link MvInRange}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class MvInRangeConstantLongEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvInRangeConstantLongEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator field;

  private final long lower;

  private final long upper;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvInRangeConstantLongEvaluator(Source source, ExpressionEvaluator field, long lower,
      long upper, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.lower = lower;
    this.upper = upper;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock fieldBlock = (LongBlock) field.eval(page)) {
      return eval(page.getPositionCount(), fieldBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, LongBlock fieldBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(MvInRange.processConstantLong(p, fieldBlock, this.lower, this.upper));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvInRangeConstantLongEvaluator[" + "field=" + field + ", lower=" + lower + ", upper=" + upper + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory field;

    private final long lower;

    private final long upper;

    public Factory(Source source, ExpressionEvaluator.Factory field, long lower, long upper) {
      this.source = source;
      this.field = field;
      this.lower = lower;
      this.upper = upper;
    }

    @Override
    public MvInRangeConstantLongEvaluator get(DriverContext context) {
      return new MvInRangeConstantLongEvaluator(source, field.get(context), lower, upper, context);
    }

    @Override
    public String toString() {
      return "MvInRangeConstantLongEvaluator[" + "field=" + field + ", lower=" + lower + ", upper=" + upper + "]";
    }
  }
}
