// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Space}.
 * This class is generated. Do not edit it.
 */
public final class SpaceEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final BreakingBytesRefBuilder scratch;

  private final EvalOperator.ExpressionEvaluator number;

  private final DriverContext driverContext;

  private Warnings warnings;

  public SpaceEvaluator(Source source, BreakingBytesRefBuilder scratch,
      EvalOperator.ExpressionEvaluator number, DriverContext driverContext) {
    this.source = source;
    this.scratch = scratch;
    this.number = number;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock numberBlock = (IntBlock) number.eval(page)) {
      IntVector numberVector = numberBlock.asVector();
      if (numberVector == null) {
        return eval(page.getPositionCount(), numberBlock);
      }
      return eval(page.getPositionCount(), numberVector);
    }
  }

  public BytesRefBlock eval(int positionCount, IntBlock numberBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (numberBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (numberBlock.getValueCount(p) != 1) {
          if (numberBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendBytesRef(Space.process(this.scratch, numberBlock.getInt(numberBlock.getFirstValueIndex(p))));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, IntVector numberVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBytesRef(Space.process(this.scratch, numberVector.getInt(p)));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "SpaceEvaluator[" + "number=" + number + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(scratch, number);
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

    private final Function<DriverContext, BreakingBytesRefBuilder> scratch;

    private final EvalOperator.ExpressionEvaluator.Factory number;

    public Factory(Source source, Function<DriverContext, BreakingBytesRefBuilder> scratch,
        EvalOperator.ExpressionEvaluator.Factory number) {
      this.source = source;
      this.scratch = scratch;
      this.number = number;
    }

    @Override
    public SpaceEvaluator get(DriverContext context) {
      return new SpaceEvaluator(source, scratch.apply(context), number.get(context), context);
    }

    @Override
    public String toString() {
      return "SpaceEvaluator[" + "number=" + number + "]";
    }
  }
}
