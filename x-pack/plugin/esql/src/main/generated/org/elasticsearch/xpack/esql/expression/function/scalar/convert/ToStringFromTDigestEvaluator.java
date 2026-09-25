// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ToString}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToStringFromTDigestEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToStringFromTDigestEvaluator.class);

  private final ExpressionEvaluator digest;

  public ToStringFromTDigestEvaluator(Source source, ExpressionEvaluator digest,
      DriverContext driverContext) {
    super(driverContext, source);
    this.digest = digest;
  }

  @Override
  public ExpressionEvaluator next() {
    return digest;
  }

  @Override
  public Block evalVector(Vector v) {
    throw new UnsupportedOperationException("vectors are unsupported for this evaluator");
  }

  @Override
  public Block evalBlock(Block b) {
    TDigestBlock block = (TDigestBlock) b;
    int positionCount = block.getPositionCount();
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      TDigestHolder scratchPad = new TDigestHolder();
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

  private BytesRef evalValue(TDigestBlock container, int index, TDigestHolder scratchPad) {
    TDigestHolder value = container.getTDigestHolder(index, scratchPad);
    return ToString.fromTDigest(value);
  }

  @Override
  public String toString() {
    return "ToStringFromTDigestEvaluator[" + "digest=" + digest + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(digest);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += digest.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory digest;

    public Factory(Source source, ExpressionEvaluator.Factory digest) {
      this.source = source;
      this.digest = digest;
    }

    @Override
    public ToStringFromTDigestEvaluator get(DriverContext context) {
      return new ToStringFromTDigestEvaluator(source, digest.get(context), context);
    }

    @Override
    public String toString() {
      return "ToStringFromTDigestEvaluator[" + "digest=" + digest + "]";
    }
  }
}
