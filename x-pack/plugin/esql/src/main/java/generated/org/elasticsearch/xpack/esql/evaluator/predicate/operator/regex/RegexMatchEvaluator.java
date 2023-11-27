// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link RegexMatch}.
 * This class is generated. Do not edit it.
 */
public final class RegexMatchEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator input;

  private final CharacterRunAutomaton pattern;

  private final DriverContext driverContext;

  public RegexMatchEvaluator(EvalOperator.ExpressionEvaluator input, CharacterRunAutomaton pattern,
      DriverContext driverContext) {
    this.input = input;
    this.pattern = pattern;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock inputBlock = (BytesRefBlock) input.eval(page)) {
      BytesRefVector inputVector = inputBlock.asVector();
      if (inputVector == null) {
        return eval(page.getPositionCount(), inputBlock);
      }
      return eval(page.getPositionCount(), inputVector).asBlock();
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock inputBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef inputScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (inputBlock.isNull(p) || inputBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        result.appendBoolean(RegexMatch.process(inputBlock.getBytesRef(inputBlock.getFirstValueIndex(p), inputScratch), pattern));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BytesRefVector inputVector) {
    try(BooleanVector.Builder result = driverContext.blockFactory().newBooleanVectorBuilder(positionCount)) {
      BytesRef inputScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(RegexMatch.process(inputVector.getBytesRef(p, inputScratch), pattern));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RegexMatchEvaluator[" + "input=" + input + ", pattern=" + pattern + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(input);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory input;

    private final CharacterRunAutomaton pattern;

    public Factory(EvalOperator.ExpressionEvaluator.Factory input, CharacterRunAutomaton pattern) {
      this.input = input;
      this.pattern = pattern;
    }

    @Override
    public RegexMatchEvaluator get(DriverContext context) {
      return new RegexMatchEvaluator(input.get(context), pattern, context);
    }

    @Override
    public String toString() {
      return "RegexMatchEvaluator[" + "input=" + input + ", pattern=" + pattern + "]";
    }
  }
}
