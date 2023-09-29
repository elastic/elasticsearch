// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.ArithmeticException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Sqrt}.
 * This class is generated. Do not edit it.
 */
public final class SqrtIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator val;

  private final DriverContext driverContext;

  public SqrtIntEvaluator(Source source, EvalOperator.ExpressionEvaluator val,
      DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.val = val;
    this.driverContext = driverContext;
  }

  @Override
  public Block.Ref eval(Page page) {
    try (Block.Ref valRef = val.eval(page)) {
      if (valRef.block().areAllValuesNull()) {
        return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
      }
      IntBlock valBlock = (IntBlock) valRef.block();
      IntVector valVector = valBlock.asVector();
      if (valVector == null) {
        return Block.Ref.floating(eval(page.getPositionCount(), valBlock));
      }
      return Block.Ref.floating(eval(page.getPositionCount(), valVector));
    }
  }

  public DoubleBlock eval(int positionCount, IntBlock valBlock) {
    DoubleBlock.Builder result = DoubleBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (valBlock.isNull(p) || valBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      try {
        result.appendDouble(Sqrt.process(valBlock.getInt(valBlock.getFirstValueIndex(p))));
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        result.appendNull();
      }
    }
    return result.build();
  }

  public DoubleBlock eval(int positionCount, IntVector valVector) {
    DoubleBlock.Builder result = DoubleBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      try {
        result.appendDouble(Sqrt.process(valVector.getInt(p)));
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        result.appendNull();
      }
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "SqrtIntEvaluator[" + "val=" + val + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val);
  }
}
