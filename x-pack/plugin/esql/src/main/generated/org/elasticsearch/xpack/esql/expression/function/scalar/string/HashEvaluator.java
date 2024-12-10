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
 * This class is generated. Do not edit it.
 */
public final class HashEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final BreakingBytesRefBuilder scratch;

  private final EvalOperator.ExpressionEvaluator alg;

  private final EvalOperator.ExpressionEvaluator input;

  private final DriverContext driverContext;

  private Warnings warnings;

  public HashEvaluator(Source source, BreakingBytesRefBuilder scratch,
      EvalOperator.ExpressionEvaluator alg, EvalOperator.ExpressionEvaluator input,
      DriverContext driverContext) {
    this.source = source;
    this.scratch = scratch;
    this.alg = alg;
    this.input = input;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock algBlock = (BytesRefBlock) alg.eval(page)) {
      try (BytesRefBlock inputBlock = (BytesRefBlock) input.eval(page)) {
        BytesRefVector algVector = algBlock.asVector();
        if (algVector == null) {
          return eval(page.getPositionCount(), algBlock, inputBlock);
        }
        BytesRefVector inputVector = inputBlock.asVector();
        if (inputVector == null) {
          return eval(page.getPositionCount(), algBlock, inputBlock);
        }
        return eval(page.getPositionCount(), algVector, inputVector);
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock algBlock, BytesRefBlock inputBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef algScratch = new BytesRef();
      BytesRef inputScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (algBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (algBlock.getValueCount(p) != 1) {
          if (algBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (inputBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (inputBlock.getValueCount(p) != 1) {
          if (inputBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendBytesRef(Hash.process(this.scratch, algBlock.getBytesRef(algBlock.getFirstValueIndex(p), algScratch), inputBlock.getBytesRef(inputBlock.getFirstValueIndex(p), inputScratch)));
        } catch (NoSuchAlgorithmException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector algVector,
      BytesRefVector inputVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef algScratch = new BytesRef();
      BytesRef inputScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBytesRef(Hash.process(this.scratch, algVector.getBytesRef(p, algScratch), inputVector.getBytesRef(p, inputScratch)));
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
    return "HashEvaluator[" + "alg=" + alg + ", input=" + input + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(scratch, alg, input);
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

    private final EvalOperator.ExpressionEvaluator.Factory alg;

    private final EvalOperator.ExpressionEvaluator.Factory input;

    public Factory(Source source, Function<DriverContext, BreakingBytesRefBuilder> scratch,
        EvalOperator.ExpressionEvaluator.Factory alg,
        EvalOperator.ExpressionEvaluator.Factory input) {
      this.source = source;
      this.scratch = scratch;
      this.alg = alg;
      this.input = input;
    }

    @Override
    public HashEvaluator get(DriverContext context) {
      return new HashEvaluator(source, scratch.apply(context), alg.get(context), input.get(context), context);
    }

    @Override
    public String toString() {
      return "HashEvaluator[" + "alg=" + alg + ", input=" + input + "]";
    }
  }
}
