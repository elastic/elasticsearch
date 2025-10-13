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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
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
public final class ClampMinIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ClampMinIntEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator field;

  private final EvalOperator.ExpressionEvaluator min;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ClampMinIntEvaluator(Source source, EvalOperator.ExpressionEvaluator field,
      EvalOperator.ExpressionEvaluator min, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.min = min;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock fieldBlock = (IntBlock) field.eval(page)) {
      try (IntBlock minBlock = (IntBlock) min.eval(page)) {
        IntVector fieldVector = fieldBlock.asVector();
        if (fieldVector == null) {
          return eval(page.getPositionCount(), fieldBlock, minBlock);
        }
        IntVector minVector = minBlock.asVector();
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

  public IntBlock eval(int positionCount, IntBlock fieldBlock, IntBlock minBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (fieldBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (fieldBlock.getValueCount(p) != 1) {
          if (fieldBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (minBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (minBlock.getValueCount(p) != 1) {
          if (minBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        int field = fieldBlock.getInt(fieldBlock.getFirstValueIndex(p));
        int min = minBlock.getInt(minBlock.getFirstValueIndex(p));
        result.appendInt(ClampMin.process(field, min));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, IntVector fieldVector, IntVector minVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        int field = fieldVector.getInt(p);
        int min = minVector.getInt(p);
        result.appendInt(p, ClampMin.process(field, min));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ClampMinIntEvaluator[" + "field=" + field + ", min=" + min + "]";
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
    public ClampMinIntEvaluator get(DriverContext context) {
      return new ClampMinIntEvaluator(source, field.get(context), min.get(context), context);
    }

    @Override
    public String toString() {
      return "ClampMinIntEvaluator[" + "field=" + field + ", min=" + min + "]";
    }
  }
}
