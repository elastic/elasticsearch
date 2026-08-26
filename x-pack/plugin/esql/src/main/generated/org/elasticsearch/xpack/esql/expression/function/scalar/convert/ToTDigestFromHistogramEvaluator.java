// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ToTDigest}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToTDigestFromHistogramEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToTDigestFromHistogramEvaluator.class);

  private final ExpressionEvaluator in;

  private final EncodedTDigest decoder;

  private final TDigestHolder scratch;

  public ToTDigestFromHistogramEvaluator(Source source, ExpressionEvaluator in,
      EncodedTDigest decoder, TDigestHolder scratch, DriverContext driverContext) {
    super(driverContext, source);
    this.in = in;
    this.decoder = decoder;
    this.scratch = scratch;
  }

  @Override
  public ExpressionEvaluator next() {
    return in;
  }

  @Override
  public Block evalVector(Vector v) {
    BytesRefVector vector = (BytesRefVector) v;
    int positionCount = v.getPositionCount();
    BytesRef scratchPad = new BytesRef();
    if (vector.isConstant()) {
      try {
        return driverContext.blockFactory().newConstantTDigestBlockWith(evalValue(vector, 0, scratchPad), positionCount);
      } catch (IllegalArgumentException  e) {
        registerException(e);
        return driverContext.blockFactory().newConstantNullBlock(positionCount);
      }
    }
    try (TDigestBlock.Builder builder = driverContext.blockFactory().newTDigestBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        try {
          builder.appendTDigest(evalValue(vector, p, scratchPad));
        } catch (IllegalArgumentException  e) {
          registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  private TDigestHolder evalValue(BytesRefVector container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return ToTDigest.fromHistogram(value, this.decoder, this.scratch);
  }

  @Override
  public Block evalBlock(Block b) {
    BytesRefBlock block = (BytesRefBlock) b;
    int positionCount = block.getPositionCount();
    try (TDigestBlock.Builder builder = driverContext.blockFactory().newTDigestBlockBuilder(positionCount)) {
      BytesRef scratchPad = new BytesRef();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          try {
            TDigestHolder value = evalValue(block, i, scratchPad);
            if (positionOpened == false && valueCount > 1) {
              builder.beginPositionEntry();
              positionOpened = true;
            }
            builder.appendTDigest(value);
            valuesAppended = true;
          } catch (IllegalArgumentException  e) {
            registerException(e);
          }
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

  private TDigestHolder evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return ToTDigest.fromHistogram(value, this.decoder, this.scratch);
  }

  @Override
  public String toString() {
    return "ToTDigestFromHistogramEvaluator[" + "in=" + in + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(in);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += in.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory in;

    private final Function<DriverContext, EncodedTDigest> decoder;

    private final Function<DriverContext, TDigestHolder> scratch;

    public Factory(Source source, ExpressionEvaluator.Factory in,
        Function<DriverContext, EncodedTDigest> decoder,
        Function<DriverContext, TDigestHolder> scratch) {
      this.source = source;
      this.in = in;
      this.decoder = decoder;
      this.scratch = scratch;
    }

    @Override
    public ToTDigestFromHistogramEvaluator get(DriverContext context) {
      return new ToTDigestFromHistogramEvaluator(source, in.get(context), decoder.apply(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "ToTDigestFromHistogramEvaluator[" + "in=" + in + "]";
    }
  }
}
