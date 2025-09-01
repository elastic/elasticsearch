// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.fulltext;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
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
public final class DecayIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DecayIntEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator value;

  private final EvalOperator.ExpressionEvaluator origin;

  private final EvalOperator.ExpressionEvaluator scale;

  private final EvalOperator.ExpressionEvaluator offset;

  private final EvalOperator.ExpressionEvaluator decay;

  private final EvalOperator.ExpressionEvaluator functionType;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DecayIntEvaluator(Source source, EvalOperator.ExpressionEvaluator value,
      EvalOperator.ExpressionEvaluator origin, EvalOperator.ExpressionEvaluator scale,
      EvalOperator.ExpressionEvaluator offset, EvalOperator.ExpressionEvaluator decay,
      EvalOperator.ExpressionEvaluator functionType, DriverContext driverContext) {
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
      try (IntBlock originBlock = (IntBlock) origin.eval(page)) {
        try (IntBlock scaleBlock = (IntBlock) scale.eval(page)) {
          try (IntBlock offsetBlock = (IntBlock) offset.eval(page)) {
            try (DoubleBlock decayBlock = (DoubleBlock) decay.eval(page)) {
              try (BytesRefBlock functionTypeBlock = (BytesRefBlock) functionType.eval(page)) {
                IntVector valueVector = valueBlock.asVector();
                if (valueVector == null) {
                  return eval(page.getPositionCount(), valueBlock, originBlock, scaleBlock, offsetBlock, decayBlock, functionTypeBlock);
                }
                IntVector originVector = originBlock.asVector();
                if (originVector == null) {
                  return eval(page.getPositionCount(), valueBlock, originBlock, scaleBlock, offsetBlock, decayBlock, functionTypeBlock);
                }
                IntVector scaleVector = scaleBlock.asVector();
                if (scaleVector == null) {
                  return eval(page.getPositionCount(), valueBlock, originBlock, scaleBlock, offsetBlock, decayBlock, functionTypeBlock);
                }
                IntVector offsetVector = offsetBlock.asVector();
                if (offsetVector == null) {
                  return eval(page.getPositionCount(), valueBlock, originBlock, scaleBlock, offsetBlock, decayBlock, functionTypeBlock);
                }
                DoubleVector decayVector = decayBlock.asVector();
                if (decayVector == null) {
                  return eval(page.getPositionCount(), valueBlock, originBlock, scaleBlock, offsetBlock, decayBlock, functionTypeBlock);
                }
                BytesRefVector functionTypeVector = functionTypeBlock.asVector();
                if (functionTypeVector == null) {
                  return eval(page.getPositionCount(), valueBlock, originBlock, scaleBlock, offsetBlock, decayBlock, functionTypeBlock);
                }
                return eval(page.getPositionCount(), valueVector, originVector, scaleVector, offsetVector, decayVector, functionTypeVector).asBlock();
              }
            }
          }
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += value.baseRamBytesUsed();
    baseRamBytesUsed += origin.baseRamBytesUsed();
    baseRamBytesUsed += scale.baseRamBytesUsed();
    baseRamBytesUsed += offset.baseRamBytesUsed();
    baseRamBytesUsed += decay.baseRamBytesUsed();
    baseRamBytesUsed += functionType.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, IntBlock valueBlock, IntBlock originBlock,
      IntBlock scaleBlock, IntBlock offsetBlock, DoubleBlock decayBlock,
      BytesRefBlock functionTypeBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      BytesRef functionTypeScratch = new BytesRef();
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
        if (originBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (originBlock.getValueCount(p) != 1) {
          if (originBlock.getValueCount(p) > 1) {
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
        if (offsetBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (offsetBlock.getValueCount(p) != 1) {
          if (offsetBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (decayBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (decayBlock.getValueCount(p) != 1) {
          if (decayBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (functionTypeBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (functionTypeBlock.getValueCount(p) != 1) {
          if (functionTypeBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendDouble(Decay.process(valueBlock.getInt(valueBlock.getFirstValueIndex(p)), originBlock.getInt(originBlock.getFirstValueIndex(p)), scaleBlock.getInt(scaleBlock.getFirstValueIndex(p)), offsetBlock.getInt(offsetBlock.getFirstValueIndex(p)), decayBlock.getDouble(decayBlock.getFirstValueIndex(p)), functionTypeBlock.getBytesRef(functionTypeBlock.getFirstValueIndex(p), functionTypeScratch)));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, IntVector valueVector, IntVector originVector,
      IntVector scaleVector, IntVector offsetVector, DoubleVector decayVector,
      BytesRefVector functionTypeVector) {
    try(DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      BytesRef functionTypeScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(p, Decay.process(valueVector.getInt(p), originVector.getInt(p), scaleVector.getInt(p), offsetVector.getInt(p), decayVector.getDouble(p), functionTypeVector.getBytesRef(p, functionTypeScratch)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DecayIntEvaluator[" + "value=" + value + ", origin=" + origin + ", scale=" + scale + ", offset=" + offset + ", decay=" + decay + ", functionType=" + functionType + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(value, origin, scale, offset, decay, functionType);
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

    private final EvalOperator.ExpressionEvaluator.Factory origin;

    private final EvalOperator.ExpressionEvaluator.Factory scale;

    private final EvalOperator.ExpressionEvaluator.Factory offset;

    private final EvalOperator.ExpressionEvaluator.Factory decay;

    private final EvalOperator.ExpressionEvaluator.Factory functionType;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory value,
        EvalOperator.ExpressionEvaluator.Factory origin,
        EvalOperator.ExpressionEvaluator.Factory scale,
        EvalOperator.ExpressionEvaluator.Factory offset,
        EvalOperator.ExpressionEvaluator.Factory decay,
        EvalOperator.ExpressionEvaluator.Factory functionType) {
      this.source = source;
      this.value = value;
      this.origin = origin;
      this.scale = scale;
      this.offset = offset;
      this.decay = decay;
      this.functionType = functionType;
    }

    @Override
    public DecayIntEvaluator get(DriverContext context) {
      return new DecayIntEvaluator(source, value.get(context), origin.get(context), scale.get(context), offset.get(context), decay.get(context), functionType.get(context), context);
    }

    @Override
    public String toString() {
      return "DecayIntEvaluator[" + "value=" + value + ", origin=" + origin + ", scale=" + scale + ", offset=" + offset + ", decay=" + decay + ", functionType=" + functionType + "]";
    }
  }
}
