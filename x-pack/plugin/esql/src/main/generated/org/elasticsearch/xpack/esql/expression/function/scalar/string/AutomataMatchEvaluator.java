// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link AutomataMatch}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class AutomataMatchEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(AutomataMatchEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator input;

  private final ByteRunAutomaton automaton;

  private final String pattern;

  private final DriverContext driverContext;

  private Warnings warnings;

  public AutomataMatchEvaluator(Source source, EvalOperator.ExpressionEvaluator input,
      ByteRunAutomaton automaton, String pattern, DriverContext driverContext) {
    this.source = source;
    this.input = input;
    this.automaton = automaton;
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

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += input.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock inputBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef inputScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (inputBlock.getValueCount(p)) {
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
        BytesRef input = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(p), inputScratch);
        result.appendBoolean(AutomataMatch.process(input, this.automaton, this.pattern));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BytesRefVector inputVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      BytesRef inputScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef input = inputVector.getBytesRef(p, inputScratch);
        result.appendBoolean(p, AutomataMatch.process(input, this.automaton, this.pattern));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "AutomataMatchEvaluator[" + "input=" + input + ", pattern=" + pattern + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(input);
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

    private final EvalOperator.ExpressionEvaluator.Factory input;

    private final ByteRunAutomaton automaton;

    private final String pattern;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory input,
        ByteRunAutomaton automaton, String pattern) {
      this.source = source;
      this.input = input;
      this.automaton = automaton;
      this.pattern = pattern;
    }

    @Override
    public AutomataMatchEvaluator get(DriverContext context) {
      return new AutomataMatchEvaluator(source, input.get(context), automaton, pattern, context);
    }

    @Override
    public String toString() {
      return "AutomataMatchEvaluator[" + "input=" + input + ", pattern=" + pattern + "]";
    }
  }
}
