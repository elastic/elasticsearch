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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ClampMax}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ClampMaxIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ClampMaxIntEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator field;

  private final EvalOperator.ExpressionEvaluator max;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ClampMaxIntEvaluator(Source source, EvalOperator.ExpressionEvaluator field,
      EvalOperator.ExpressionEvaluator max, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.max = max;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock fieldBlock = (IntBlock) field.eval(page)) {
      try (IntBlock maxBlock = (IntBlock) max.eval(page)) {
        IntVector fieldVector = fieldBlock.asVector();
        if (fieldVector == null) {
          return eval(page.getPositionCount(), fieldBlock, maxBlock);
        }
        IntVector maxVector = maxBlock.asVector();
        if (maxVector == null) {
          return eval(page.getPositionCount(), fieldBlock, maxBlock);
        }
        return eval(page.getPositionCount(), fieldVector, maxVector).asBlock();
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    baseRamBytesUsed += max.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, IntBlock fieldBlock, IntBlock maxBlock) {
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
        if (maxBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (maxBlock.getValueCount(p) != 1) {
          if (maxBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        int field = fieldBlock.getInt(fieldBlock.getFirstValueIndex(p));
        int max = maxBlock.getInt(maxBlock.getFirstValueIndex(p));
        result.appendInt(ClampMax.process(field, max));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, IntVector fieldVector, IntVector maxVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        int field = fieldVector.getInt(p);
        int max = maxVector.getInt(p);
        result.appendInt(p, ClampMax.process(field, max));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ClampMaxIntEvaluator[" + "field=" + field + ", max=" + max + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field, max);
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

    private final EvalOperator.ExpressionEvaluator.Factory max;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field,
        EvalOperator.ExpressionEvaluator.Factory max) {
      this.source = source;
      this.field = field;
      this.max = max;
    }

    @Override
    public ClampMaxIntEvaluator get(DriverContext context) {
      return new ClampMaxIntEvaluator(source, field.get(context), max.get(context), context);
    }

    @Override
    public String toString() {
      return "ClampMaxIntEvaluator[" + "field=" + field + ", max=" + max + "]";
    }
  }
}
