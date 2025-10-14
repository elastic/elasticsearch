// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.score;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Decay}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DecayDateNanosEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DecayDateNanosEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator value;

  private final long origin;

  private final long scale;

  private final long offset;

  private final double decay;

  private final Decay.DecayFunction decayFunction;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DecayDateNanosEvaluator(Source source, EvalOperator.ExpressionEvaluator value, long origin,
      long scale, long offset, double decay, Decay.DecayFunction decayFunction,
      DriverContext driverContext) {
    this.source = source;
    this.value = value;
    this.origin = origin;
    this.scale = scale;
    this.offset = offset;
    this.decay = decay;
    this.decayFunction = decayFunction;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock valueBlock = (LongBlock) value.eval(page)) {
      LongVector valueVector = valueBlock.asVector();
      if (valueVector == null) {
        return eval(page.getPositionCount(), valueBlock);
      }
      return eval(page.getPositionCount(), valueVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += value.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, LongBlock valueBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (valueBlock.getValueCount(p)) {
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
        long value = valueBlock.getLong(valueBlock.getFirstValueIndex(p));
        try {
          result.appendDouble(Decay.processDateNanos(value, this.origin, this.scale, this.offset, this.decay, this.decayFunction));
        } catch (InvalidArgumentException | IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, LongVector valueVector) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long value = valueVector.getLong(p);
        try {
          result.appendDouble(Decay.processDateNanos(value, this.origin, this.scale, this.offset, this.decay, this.decayFunction));
        } catch (InvalidArgumentException | IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DecayDateNanosEvaluator[" + "value=" + value + ", origin=" + origin + ", scale=" + scale + ", offset=" + offset + ", decay=" + decay + ", decayFunction=" + decayFunction + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(value);
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

    private final EvalOperator.ExpressionEvaluator.Factory value;

    private final long origin;

    private final long scale;

    private final long offset;

    private final double decay;

    private final Decay.DecayFunction decayFunction;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory value, long origin,
        long scale, long offset, double decay, Decay.DecayFunction decayFunction) {
      this.source = source;
      this.value = value;
      this.origin = origin;
      this.scale = scale;
      this.offset = offset;
      this.decay = decay;
      this.decayFunction = decayFunction;
    }

    @Override
    public DecayDateNanosEvaluator get(DriverContext context) {
      return new DecayDateNanosEvaluator(source, value.get(context), origin, scale, offset, decay, decayFunction, context);
    }

    @Override
    public String toString() {
      return "DecayDateNanosEvaluator[" + "value=" + value + ", origin=" + origin + ", scale=" + scale + ", offset=" + offset + ", decay=" + decay + ", decayFunction=" + decayFunction + "]";
    }
  }
}
