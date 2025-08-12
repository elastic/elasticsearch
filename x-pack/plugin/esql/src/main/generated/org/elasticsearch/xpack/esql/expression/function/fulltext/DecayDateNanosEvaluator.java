// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.fulltext;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
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
public final class DecayDateNanosEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator value;

  private final EvalOperator.ExpressionEvaluator origin;

  private final EvalOperator.ExpressionEvaluator scale;

  private final EvalOperator.ExpressionEvaluator offset;

  private final EvalOperator.ExpressionEvaluator decay;

  private final EvalOperator.ExpressionEvaluator functionType;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DecayDateNanosEvaluator(Source source, EvalOperator.ExpressionEvaluator value,
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
    try (LongBlock valueBlock = (LongBlock) value.eval(page)) {
      try (LongBlock originBlock = (LongBlock) origin.eval(page)) {
        try (BytesRefBlock scaleBlock = (BytesRefBlock) scale.eval(page)) {
          try (BytesRefBlock offsetBlock = (BytesRefBlock) offset.eval(page)) {
            try (DoubleBlock decayBlock = (DoubleBlock) decay.eval(page)) {
              try (BytesRefBlock functionTypeBlock = (BytesRefBlock) functionType.eval(page)) {
                LongVector valueVector = valueBlock.asVector();
                if (valueVector == null) {
                  return eval(page.getPositionCount(), valueBlock, originBlock, scaleBlock, offsetBlock, decayBlock, functionTypeBlock);
                }
                LongVector originVector = originBlock.asVector();
                if (originVector == null) {
                  return eval(page.getPositionCount(), valueBlock, originBlock, scaleBlock, offsetBlock, decayBlock, functionTypeBlock);
                }
                BytesRefVector scaleVector = scaleBlock.asVector();
                if (scaleVector == null) {
                  return eval(page.getPositionCount(), valueBlock, originBlock, scaleBlock, offsetBlock, decayBlock, functionTypeBlock);
                }
                BytesRefVector offsetVector = offsetBlock.asVector();
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

  public DoubleBlock eval(int positionCount, LongBlock valueBlock, LongBlock originBlock,
      BytesRefBlock scaleBlock, BytesRefBlock offsetBlock, DoubleBlock decayBlock,
      BytesRefBlock functionTypeBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      BytesRef scaleScratch = new BytesRef();
      BytesRef offsetScratch = new BytesRef();
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
        result.appendDouble(Decay.processDateNanos(valueBlock.getLong(valueBlock.getFirstValueIndex(p)), originBlock.getLong(originBlock.getFirstValueIndex(p)), scaleBlock.getBytesRef(scaleBlock.getFirstValueIndex(p), scaleScratch), offsetBlock.getBytesRef(offsetBlock.getFirstValueIndex(p), offsetScratch), decayBlock.getDouble(decayBlock.getFirstValueIndex(p)), functionTypeBlock.getBytesRef(functionTypeBlock.getFirstValueIndex(p), functionTypeScratch)));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, LongVector valueVector, LongVector originVector,
      BytesRefVector scaleVector, BytesRefVector offsetVector, DoubleVector decayVector,
      BytesRefVector functionTypeVector) {
    try(DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      BytesRef scaleScratch = new BytesRef();
      BytesRef offsetScratch = new BytesRef();
      BytesRef functionTypeScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(p, Decay.processDateNanos(valueVector.getLong(p), originVector.getLong(p), scaleVector.getBytesRef(p, scaleScratch), offsetVector.getBytesRef(p, offsetScratch), decayVector.getDouble(p), functionTypeVector.getBytesRef(p, functionTypeScratch)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DecayDateNanosEvaluator[" + "value=" + value + ", origin=" + origin + ", scale=" + scale + ", offset=" + offset + ", decay=" + decay + ", functionType=" + functionType + "]";
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
    public DecayDateNanosEvaluator get(DriverContext context) {
      return new DecayDateNanosEvaluator(source, value.get(context), origin.get(context), scale.get(context), offset.get(context), decay.get(context), functionType.get(context), context);
    }

    @Override
    public String toString() {
      return "DecayDateNanosEvaluator[" + "value=" + value + ", origin=" + origin + ", scale=" + scale + ", offset=" + offset + ", decay=" + decay + ", functionType=" + functionType + "]";
    }
  }
}
