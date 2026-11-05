// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.ArithmeticException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link MvMedianAbsoluteDeviation}.
 * This class is generated. Edit {@code MvEvaluatorImplementer} instead.
 */
public final class MvMedianAbsoluteDeviationDoubleEvaluator extends AbstractMultivalueFunction.AbstractNullableEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvMedianAbsoluteDeviationDoubleEvaluator.class);

  private final Source source;

  private Warnings warnings;

  public MvMedianAbsoluteDeviationDoubleEvaluator(Source source, ExpressionEvaluator field,
      DriverContext driverContext) {
    super(driverContext, field);
    this.source = source;
  }

  @Override
  public String name() {
    return "MvMedianAbsoluteDeviation";
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block evalNullable(Block fieldVal) {
    if (fieldVal.mvSortedAscending()) {
      return evalAscendingNullable(fieldVal);
    }
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Doubles work = new MvMedianAbsoluteDeviation.Doubles();
      for (int p = 0; p < positionCount; p++) {
        if (v.isNull(p)) {
          builder.appendNull();
          continue;
        }
        int valueCount = v.getValueCount(p);
        try {
          int first = v.getFirstValueIndex(p);
          if (valueCount == 1) {
            double value = v.getDouble(first);
            double result = MvMedianAbsoluteDeviation.single(value);
            builder.appendDouble(result);
            continue;
          }
          int end = first + valueCount;
          for (int i = first; i < end; i++) {
            double value = v.getDouble(i);
            MvMedianAbsoluteDeviation.process(work, value);
          }
          double result = MvMedianAbsoluteDeviation.finish(work);
          builder.appendDouble(result);
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing only single valued fields.
   */
  @Override
  public Block evalSingleValuedNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Doubles work = new MvMedianAbsoluteDeviation.Doubles();
      for (int p = 0; p < positionCount; p++) {
        if (v.isNull(p)) {
          builder.appendNull();
          continue;
        }
        int valueCount = v.getValueCount(p);
        try {
          assert valueCount == 1;
          int first = v.getFirstValueIndex(p);
          double value = v.getDouble(first);
          double result = MvMedianAbsoluteDeviation.single(value);
          builder.appendDouble(result);
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Doubles work = new MvMedianAbsoluteDeviation.Doubles();
      for (int p = 0; p < positionCount; p++) {
        if (v.isNull(p)) {
          builder.appendNull();
          continue;
        }
        int valueCount = v.getValueCount(p);
        try {
          int first = v.getFirstValueIndex(p);
          double result = MvMedianAbsoluteDeviation.ascending(work, v, first, valueCount);
          builder.appendDouble(result);
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  @Override
  public long baseRamBytesUsed() {
    return BASE_RAM_BYTES_USED + field.baseRamBytesUsed();
  }

  public static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory field;

    public Factory(Source source, ExpressionEvaluator.Factory field) {
      this.source = source;
      this.field = field;
    }

    @Override
    public MvMedianAbsoluteDeviationDoubleEvaluator get(DriverContext context) {
      return new MvMedianAbsoluteDeviationDoubleEvaluator(source, field.get(context), context);
    }

    @Override
    public String toString() {
      return "MvMedianAbsoluteDeviation[field=" + field + "]";
    }
  }
}
