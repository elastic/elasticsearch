// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link UrlEncode}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class UrlEncodeEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(UrlEncodeEvaluator.class);

  private final EvalOperator.ExpressionEvaluator val;

  private final BreakingBytesRefBuilder scratch;

  public UrlEncodeEvaluator(Source source, EvalOperator.ExpressionEvaluator val,
      BreakingBytesRefBuilder scratch, DriverContext driverContext) {
    super(driverContext, source);
    this.val = val;
    this.scratch = scratch;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return val;
  }

  @Override
  public Block evalVector(Vector v) {
    BytesRefVector vector = (BytesRefVector) v;
    OrdinalBytesRefVector ordinals = vector.asOrdinals();
    if (ordinals != null) {
      return evalOrdinals(ordinals);
    }
    int positionCount = v.getPositionCount();
    BytesRef scratchPad = new BytesRef();
    if (vector.isConstant()) {
      return driverContext.blockFactory().newConstantBytesRefBlockWith(evalValue(vector, 0, scratchPad), positionCount);
    }
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        builder.appendBytesRef(evalValue(vector, p, scratchPad));
      }
      return builder.build();
    }
  }

  private BytesRef evalValue(BytesRefVector container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return UrlEncode.process(value, this.scratch);
  }

  @Override
  public Block evalBlock(Block b) {
    BytesRefBlock block = (BytesRefBlock) b;
    int positionCount = block.getPositionCount();
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef scratchPad = new BytesRef();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          BytesRef value = evalValue(block, i, scratchPad);
          if (positionOpened == false && valueCount > 1) {
            builder.beginPositionEntry();
            positionOpened = true;
          }
          builder.appendBytesRef(value);
          valuesAppended = true;
        }
        if (valuesAppended == false) {
          builder.appendNull();
        } else if (positionOpened) {
          builder.endPositionEntry();
        }
      }
      return builder.build();
    }
  }

  private BytesRef evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return UrlEncode.process(value, this.scratch);
  }

  private Block evalOrdinals(OrdinalBytesRefVector v) {
    int positionCount = v.getDictionaryVector().getPositionCount();
    BytesRef scratchPad = new BytesRef();
    try (BytesRefVector.Builder builder = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        builder.appendBytesRef(evalValue(v.getDictionaryVector(), p, scratchPad));
      }
      IntVector ordinals = v.getOrdinalsVector();
      ordinals.incRef();
      return new OrdinalBytesRefVector(ordinals, builder.build()).asBlock();
    }
  }

  @Override
  public String toString() {
    return "UrlEncodeEvaluator[" + "val=" + val + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val, scratch);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += val.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory val;

    private final Function<DriverContext, BreakingBytesRefBuilder> scratch;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory val,
        Function<DriverContext, BreakingBytesRefBuilder> scratch) {
      this.source = source;
      this.val = val;
      this.scratch = scratch;
    }

    @Override
    public UrlEncodeEvaluator get(DriverContext context) {
      return new UrlEncodeEvaluator(source, val.get(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "UrlEncodeEvaluator[" + "val=" + val + "]";
    }
  }
}
