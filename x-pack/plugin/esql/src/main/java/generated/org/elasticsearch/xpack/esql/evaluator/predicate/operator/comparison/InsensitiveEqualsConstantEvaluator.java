// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.qlcore.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link InsensitiveEquals}.
 * This class is generated. Do not edit it.
 */
public final class InsensitiveEqualsConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator lhs;

  private final ByteRunAutomaton rhs;

  private final DriverContext driverContext;

  public InsensitiveEqualsConstantEvaluator(Source source, EvalOperator.ExpressionEvaluator lhs,
      ByteRunAutomaton rhs, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.lhs = lhs;
    this.rhs = rhs;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock lhsBlock = (BytesRefBlock) lhs.eval(page)) {
      BytesRefVector lhsVector = lhsBlock.asVector();
      if (lhsVector == null) {
        return eval(page.getPositionCount(), lhsBlock);
      }
      return eval(page.getPositionCount(), lhsVector).asBlock();
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock lhsBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef lhsScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (lhsBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (lhsBlock.getValueCount(p) != 1) {
          if (lhsBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBoolean(InsensitiveEquals.processConstant(lhsBlock.getBytesRef(lhsBlock.getFirstValueIndex(p), lhsScratch), rhs));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BytesRefVector lhsVector) {
    try(BooleanVector.Builder result = driverContext.blockFactory().newBooleanVectorBuilder(positionCount)) {
      BytesRef lhsScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(InsensitiveEquals.processConstant(lhsVector.getBytesRef(p, lhsScratch), rhs));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "InsensitiveEqualsConstantEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(lhs);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory lhs;

    private final ByteRunAutomaton rhs;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory lhs,
        ByteRunAutomaton rhs) {
      this.source = source;
      this.lhs = lhs;
      this.rhs = rhs;
    }

    @Override
    public InsensitiveEqualsConstantEvaluator get(DriverContext context) {
      return new InsensitiveEqualsConstantEvaluator(source, lhs.get(context), rhs, context);
    }

    @Override
    public String toString() {
      return "InsensitiveEqualsConstantEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
    }
  }
}
