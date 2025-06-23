// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvSlice}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class MvSliceDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator field;

  private final EvalOperator.ExpressionEvaluator start;

  private final EvalOperator.ExpressionEvaluator end;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvSliceDoubleEvaluator(Source source, EvalOperator.ExpressionEvaluator field,
      EvalOperator.ExpressionEvaluator start, EvalOperator.ExpressionEvaluator end,
      DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.start = start;
    this.end = end;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock fieldBlock = (DoubleBlock) field.eval(page)) {
      try (IntBlock startBlock = (IntBlock) start.eval(page)) {
        try (IntBlock endBlock = (IntBlock) end.eval(page)) {
          return eval(page.getPositionCount(), fieldBlock, startBlock, endBlock);
        }
      }
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock fieldBlock, IntBlock startBlock,
      IntBlock endBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!fieldBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (startBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (startBlock.getValueCount(p) != 1) {
          if (startBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (endBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (endBlock.getValueCount(p) != 1) {
          if (endBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          MvSlice.process(result, p, fieldBlock, startBlock.getInt(startBlock.getFirstValueIndex(p)), endBlock.getInt(endBlock.getFirstValueIndex(p)));
        } catch (InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvSliceDoubleEvaluator[" + "field=" + field + ", start=" + start + ", end=" + end + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field, start, end);
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

    private final EvalOperator.ExpressionEvaluator.Factory start;

    private final EvalOperator.ExpressionEvaluator.Factory end;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field,
        EvalOperator.ExpressionEvaluator.Factory start,
        EvalOperator.ExpressionEvaluator.Factory end) {
      this.source = source;
      this.field = field;
      this.start = start;
      this.end = end;
    }

    @Override
    public MvSliceDoubleEvaluator get(DriverContext context) {
      return new MvSliceDoubleEvaluator(source, field.get(context), start.get(context), end.get(context), context);
    }

    @Override
    public String toString() {
      return "MvSliceDoubleEvaluator[" + "field=" + field + ", start=" + start + ", end=" + end + "]";
    }
  }
}
