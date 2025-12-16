// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link InsensitiveEquals}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class InsensitiveEqualsConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(InsensitiveEqualsConstantEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator lhs;

  private final ByteRunAutomaton rhs;

  private final DriverContext driverContext;

  private Warnings warnings;

  public InsensitiveEqualsConstantEvaluator(Source source, EvalOperator.ExpressionEvaluator lhs,
      ByteRunAutomaton rhs, DriverContext driverContext) {
    this.source = source;
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

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += lhs.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock lhsBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef lhsScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (lhsBlock.getValueCount(p)) {
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
        BytesRef lhs = lhsBlock.getBytesRef(lhsBlock.getFirstValueIndex(p), lhsScratch);
        result.appendBoolean(InsensitiveEquals.processConstant(lhs, this.rhs));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BytesRefVector lhsVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      BytesRef lhsScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef lhs = lhsVector.getBytesRef(p, lhsScratch);
        result.appendBoolean(p, InsensitiveEquals.processConstant(lhs, this.rhs));
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
