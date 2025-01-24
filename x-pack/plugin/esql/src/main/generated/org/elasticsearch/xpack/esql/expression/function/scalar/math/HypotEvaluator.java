// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Hypot}.
 * This class is generated. Do not edit it.
 */
public final class HypotEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator n1;

  private final EvalOperator.ExpressionEvaluator n2;

  private final DriverContext driverContext;

  private Warnings warnings;

  public HypotEvaluator(Source source, EvalOperator.ExpressionEvaluator n1,
      EvalOperator.ExpressionEvaluator n2, DriverContext driverContext) {
    this.source = source;
    this.n1 = n1;
    this.n2 = n2;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock n1Block = (DoubleBlock) n1.eval(page)) {
      try (DoubleBlock n2Block = (DoubleBlock) n2.eval(page)) {
        DoubleVector n1Vector = n1Block.asVector();
        if (n1Vector == null) {
          return eval(page.getPositionCount(), n1Block, n2Block);
        }
        DoubleVector n2Vector = n2Block.asVector();
        if (n2Vector == null) {
          return eval(page.getPositionCount(), n1Block, n2Block);
        }
        return eval(page.getPositionCount(), n1Vector, n2Vector).asBlock();
      }
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock n1Block, DoubleBlock n2Block) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (n1Block.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (n1Block.getValueCount(p) != 1) {
          if (n1Block.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (n2Block.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (n2Block.getValueCount(p) != 1) {
          if (n2Block.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendDouble(Hypot.process(n1Block.getDouble(n1Block.getFirstValueIndex(p)), n2Block.getDouble(n2Block.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, DoubleVector n1Vector, DoubleVector n2Vector) {
    try(DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(p, Hypot.process(n1Vector.getDouble(p), n2Vector.getDouble(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "HypotEvaluator[" + "n1=" + n1 + ", n2=" + n2 + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(n1, n2);
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

    private final EvalOperator.ExpressionEvaluator.Factory n1;

    private final EvalOperator.ExpressionEvaluator.Factory n2;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory n1,
        EvalOperator.ExpressionEvaluator.Factory n2) {
      this.source = source;
      this.n1 = n1;
      this.n2 = n2;
    }

    @Override
    public HypotEvaluator get(DriverContext context) {
      return new HypotEvaluator(source, n1.get(context), n2.get(context), context);
    }

    @Override
    public String toString() {
      return "HypotEvaluator[" + "n1=" + n1 + ", n2=" + n2 + "]";
    }
  }
}
