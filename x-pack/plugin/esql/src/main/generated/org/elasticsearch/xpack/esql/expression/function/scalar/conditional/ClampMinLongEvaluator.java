// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ClampMin}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ClampMinLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ClampMinLongEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator field;

  private final EvalOperator.ExpressionEvaluator min;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ClampMinLongEvaluator(Source source, EvalOperator.ExpressionEvaluator field,
      EvalOperator.ExpressionEvaluator min, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.min = min;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock fieldBlock = (LongBlock) field.eval(page)) {
      try (LongBlock minBlock = (LongBlock) min.eval(page)) {
        LongVector fieldVector = fieldBlock.asVector();
        if (fieldVector == null) {
          return eval(page.getPositionCount(), fieldBlock, minBlock);
        }
        LongVector minVector = minBlock.asVector();
        if (minVector == null) {
          return eval(page.getPositionCount(), fieldBlock, minBlock);
        }
        return eval(page.getPositionCount(), fieldVector, minVector).asBlock();
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    baseRamBytesUsed += min.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, LongBlock fieldBlock, LongBlock minBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (fieldBlock.getValueCount(p)) {
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
        switch (minBlock.getValueCount(p)) {
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
        long field = fieldBlock.getLong(fieldBlock.getFirstValueIndex(p));
        long min = minBlock.getLong(minBlock.getFirstValueIndex(p));
        result.appendLong(ClampMin.process(field, min));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector fieldVector, LongVector minVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long field = fieldVector.getLong(p);
        long min = minVector.getLong(p);
        result.appendLong(p, ClampMin.process(field, min));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ClampMinLongEvaluator[" + "field=" + field + ", min=" + min + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field, min);
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

    private final EvalOperator.ExpressionEvaluator.Factory field;

    private final EvalOperator.ExpressionEvaluator.Factory min;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field,
        EvalOperator.ExpressionEvaluator.Factory min) {
      this.source = source;
      this.field = field;
      this.min = min;
    }

    @Override
    public ClampMinLongEvaluator get(DriverContext context) {
      return new ClampMinLongEvaluator(source, field.get(context), min.get(context), context);
    }

    @Override
    public String toString() {
      return "ClampMinLongEvaluator[" + "field=" + field + ", min=" + min + "]";
    }
  }
}
