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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
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
public final class ClampMinBooleanEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ClampMinBooleanEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator field;

  private final EvalOperator.ExpressionEvaluator min;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ClampMinBooleanEvaluator(Source source, EvalOperator.ExpressionEvaluator field,
      EvalOperator.ExpressionEvaluator min, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.min = min;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BooleanBlock fieldBlock = (BooleanBlock) field.eval(page)) {
      try (BooleanBlock minBlock = (BooleanBlock) min.eval(page)) {
        BooleanVector fieldVector = fieldBlock.asVector();
        if (fieldVector == null) {
          return eval(page.getPositionCount(), fieldBlock, minBlock);
        }
        BooleanVector minVector = minBlock.asVector();
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

  public BooleanBlock eval(int positionCount, BooleanBlock fieldBlock, BooleanBlock minBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
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
        boolean field = fieldBlock.getBoolean(fieldBlock.getFirstValueIndex(p));
        boolean min = minBlock.getBoolean(minBlock.getFirstValueIndex(p));
        result.appendBoolean(ClampMin.process(field, min));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BooleanVector fieldVector, BooleanVector minVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean field = fieldVector.getBoolean(p);
        boolean min = minVector.getBoolean(p);
        result.appendBoolean(p, ClampMin.process(field, min));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ClampMinBooleanEvaluator[" + "field=" + field + ", min=" + min + "]";
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
    public ClampMinBooleanEvaluator get(DriverContext context) {
      return new ClampMinBooleanEvaluator(source, field.get(context), min.get(context), context);
    }

    @Override
    public String toString() {
      return "ClampMinBooleanEvaluator[" + "field=" + field + ", min=" + min + "]";
    }
  }
}
