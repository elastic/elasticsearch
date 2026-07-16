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
public final class MvInRangeLongEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvInRangeLongEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator field;

  private final ExpressionEvaluator lower;

  private final ExpressionEvaluator upper;

  private final boolean includeLower;

  private final boolean includeUpper;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvInRangeLongEvaluator(Source source, ExpressionEvaluator field, ExpressionEvaluator lower,
      ExpressionEvaluator upper, boolean includeLower, boolean includeUpper,
      DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.lower = lower;
    this.upper = upper;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock fieldBlock = (LongBlock) field.eval(page)) {
      try (LongBlock lowerBlock = (LongBlock) lower.eval(page)) {
        try (LongBlock upperBlock = (LongBlock) upper.eval(page)) {
          return eval(page.getPositionCount(), fieldBlock, lowerBlock, upperBlock);
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    baseRamBytesUsed += lower.baseRamBytesUsed();
    baseRamBytesUsed += upper.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, LongBlock fieldBlock, LongBlock lowerBlock,
      LongBlock upperBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(MvInRange.process(p, fieldBlock, lowerBlock, upperBlock, this.includeLower, this.includeUpper));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvInRangeLongEvaluator[" + "field=" + field + ", lower=" + lower + ", upper=" + upper + ", includeLower=" + includeLower + ", includeUpper=" + includeUpper + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field, lower, upper);
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

    private final ExpressionEvaluator.Factory lower;

    private final ExpressionEvaluator.Factory upper;

    private final boolean includeLower;

    private final boolean includeUpper;

    public Factory(Source source, ExpressionEvaluator.Factory field,
        ExpressionEvaluator.Factory lower, ExpressionEvaluator.Factory upper, boolean includeLower,
        boolean includeUpper) {
      this.source = source;
      this.field = field;
      this.lower = lower;
      this.upper = upper;
      this.includeLower = includeLower;
      this.includeUpper = includeUpper;
    }

    @Override
    public MvInRangeLongEvaluator get(DriverContext context) {
      return new MvInRangeLongEvaluator(source, field.get(context), lower.get(context), upper.get(context), includeLower, includeUpper, context);
    }

    @Override
    public String toString() {
      return "MvInRangeLongEvaluator[" + "field=" + field + ", lower=" + lower + ", upper=" + upper + ", includeLower=" + includeLower + ", includeUpper=" + includeUpper + "]";
    }
  }
}
