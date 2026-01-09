// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.security.NoSuchAlgorithmException;
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
public final class HashEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(HashEvaluator.class);

  private final Source source;

  private final BreakingBytesRefBuilder scratch;

  private final EvalOperator.ExpressionEvaluator algorithm;

  private final EvalOperator.ExpressionEvaluator input;

  private final DriverContext driverContext;

  private Warnings warnings;

  public HashEvaluator(Source source, BreakingBytesRefBuilder scratch,
      EvalOperator.ExpressionEvaluator algorithm, EvalOperator.ExpressionEvaluator input,
      DriverContext driverContext) {
    this.source = source;
    this.scratch = scratch;
    this.algorithm = algorithm;
    this.input = input;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock algorithmBlock = (BytesRefBlock) algorithm.eval(page)) {
      try (BytesRefBlock inputBlock = (BytesRefBlock) input.eval(page)) {
        BytesRefVector algorithmVector = algorithmBlock.asVector();
        if (algorithmVector == null) {
          return eval(page.getPositionCount(), algorithmBlock, inputBlock);
        }
        BytesRefVector inputVector = inputBlock.asVector();
        if (inputVector == null) {
          return eval(page.getPositionCount(), algorithmBlock, inputBlock);
        }
        return eval(page.getPositionCount(), algorithmVector, inputVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += algorithm.baseRamBytesUsed();
    baseRamBytesUsed += input.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock algorithmBlock,
      BytesRefBlock inputBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef algorithmScratch = new BytesRef();
      BytesRef inputScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (algorithmBlock.getValueCount(p)) {
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
        BytesRef algorithm = algorithmBlock.getBytesRef(algorithmBlock.getFirstValueIndex(p), algorithmScratch);
        BytesRef input = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(p), inputScratch);
        try {
          result.appendBytesRef(Hash.process(this.scratch, algorithm, input));
        } catch (NoSuchAlgorithmException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector algorithmVector,
      BytesRefVector inputVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef algorithmScratch = new BytesRef();
      BytesRef inputScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef algorithm = algorithmVector.getBytesRef(p, algorithmScratch);
        BytesRef input = inputVector.getBytesRef(p, inputScratch);
        try {
          result.appendBytesRef(Hash.process(this.scratch, algorithm, input));
        } catch (NoSuchAlgorithmException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "HashEvaluator[" + "algorithm=" + algorithm + ", input=" + input + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(scratch, algorithm, input);
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

    private final EvalOperator.ExpressionEvaluator.Factory algorithm;

    private final EvalOperator.ExpressionEvaluator.Factory input;

    public Factory(Source source, Function<DriverContext, BreakingBytesRefBuilder> scratch,
        EvalOperator.ExpressionEvaluator.Factory algorithm,
        EvalOperator.ExpressionEvaluator.Factory input) {
      this.source = source;
      this.scratch = scratch;
      this.algorithm = algorithm;
      this.input = input;
    }

    @Override
    public HashEvaluator get(DriverContext context) {
      return new HashEvaluator(source, scratch.apply(context), algorithm.get(context), input.get(context), context);
    }

    @Override
    public String toString() {
      return "HashEvaluator[" + "algorithm=" + algorithm + ", input=" + input + "]";
    }
  }
}
