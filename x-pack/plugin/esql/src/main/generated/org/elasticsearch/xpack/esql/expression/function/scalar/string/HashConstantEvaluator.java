// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Hash}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class HashConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(HashConstantEvaluator.class);

  private final Source source;

  private final BreakingBytesRefBuilder scratch;

  private final Hash.HashFunction algorithm;

  private final EvalOperator.ExpressionEvaluator input;

  private final DriverContext driverContext;

  private Warnings warnings;

  public HashConstantEvaluator(Source source, BreakingBytesRefBuilder scratch,
      Hash.HashFunction algorithm, EvalOperator.ExpressionEvaluator input,
      DriverContext driverContext) {
    this.source = source;
    this.scratch = scratch;
    this.algorithm = algorithm;
    this.input = input;
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

  public BytesRefBlock eval(int positionCount, BytesRefBlock inputBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
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
        result.appendBytesRef(Hash.processConstant(this.scratch, this.algorithm, input));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector inputVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef inputScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef input = inputVector.getBytesRef(p, inputScratch);
        result.appendBytesRef(Hash.processConstant(this.scratch, this.algorithm, input));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "HashConstantEvaluator[" + "algorithm=" + algorithm + ", input=" + input + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(scratch, input);
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

    private final Function<DriverContext, Hash.HashFunction> algorithm;

    private final EvalOperator.ExpressionEvaluator.Factory input;

    public Factory(Source source, Function<DriverContext, BreakingBytesRefBuilder> scratch,
        Function<DriverContext, Hash.HashFunction> algorithm,
        EvalOperator.ExpressionEvaluator.Factory input) {
      this.source = source;
      this.scratch = scratch;
      this.algorithm = algorithm;
      this.input = input;
    }

    @Override
    public HashConstantEvaluator get(DriverContext context) {
      return new HashConstantEvaluator(source, scratch.apply(context), algorithm.apply(context), input.get(context), context);
    }

    @Override
    public String toString() {
      return "HashConstantEvaluator[" + "algorithm=" + algorithm + ", input=" + input + "]";
    }
  }
}
