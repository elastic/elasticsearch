// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.score;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Decay}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DecayIntOriginConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DecayIntOriginConstantEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator value;

  private final int origin;

  private final EvalOperator.ExpressionEvaluator scale;

  private final int offset;

  private final double decay;

  private final BytesRef functionType;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DecayIntOriginConstantEvaluator(Source source, EvalOperator.ExpressionEvaluator value,
      int origin, EvalOperator.ExpressionEvaluator scale, int offset, double decay,
      BytesRef functionType, DriverContext driverContext) {
    this.source = source;
    this.value = value;
    this.origin = origin;
    this.scale = scale;
    this.offset = offset;
    this.decay = decay;
    this.functionType = functionType;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock valueBlock = (IntBlock) value.eval(page)) {
      try (IntBlock scaleBlock = (IntBlock) scale.eval(page)) {
        IntVector valueVector = valueBlock.asVector();
        if (valueVector == null) {
          return eval(page.getPositionCount(), valueBlock, scaleBlock);
        }
        IntVector scaleVector = scaleBlock.asVector();
        if (scaleVector == null) {
          return eval(page.getPositionCount(), valueBlock, scaleBlock);
        }
        return eval(page.getPositionCount(), valueVector, scaleVector).asBlock();
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += value.baseRamBytesUsed();
    baseRamBytesUsed += scale.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, IntBlock valueBlock, IntBlock scaleBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (valueBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (valueBlock.getValueCount(p) != 1) {
          if (valueBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (scaleBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (scaleBlock.getValueCount(p) != 1) {
          if (scaleBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendDouble(Decay.processOriginConstant(valueBlock.getInt(valueBlock.getFirstValueIndex(p)), this.origin, scaleBlock.getInt(scaleBlock.getFirstValueIndex(p)), this.offset, this.decay, this.functionType));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, IntVector valueVector, IntVector scaleVector) {
    try(DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(p, Decay.processOriginConstant(valueVector.getInt(p), this.origin, scaleVector.getInt(p), this.offset, this.decay, this.functionType));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DecayIntOriginConstantEvaluator[" + "value=" + value + ", origin=" + origin + ", scale=" + scale + ", offset=" + offset + ", decay=" + decay + ", functionType=" + functionType + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(value, scale);
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

    private final int origin;

    private final EvalOperator.ExpressionEvaluator.Factory scale;

    private final int offset;

    private final double decay;

    private final BytesRef functionType;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory value, int origin,
        EvalOperator.ExpressionEvaluator.Factory scale, int offset, double decay,
        BytesRef functionType) {
      this.source = source;
      this.value = value;
      this.origin = origin;
      this.scale = scale;
      this.offset = offset;
      this.decay = decay;
      this.functionType = functionType;
    }

    @Override
    public DecayIntOriginConstantEvaluator get(DriverContext context) {
      return new DecayIntOriginConstantEvaluator(source, value.get(context), origin, scale.get(context), offset, decay, functionType, context);
    }

    @Override
    public String toString() {
      return "DecayIntOriginConstantEvaluator[" + "value=" + value + ", origin=" + origin + ", scale=" + scale + ", offset=" + offset + ", decay=" + decay + ", functionType=" + functionType + "]";
    }
  }
}
