// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.ArithmeticException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntSingleArrayPool;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link BitLength}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class BitLengthEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BitLengthEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator val;

  private final DriverContext driverContext;

  private Warnings warnings;

  private final IntSingleArrayPool pool;

  public BitLengthEvaluator(Source source, ExpressionEvaluator val, DriverContext driverContext) {
    this.source = source;
    this.val = val;
    this.driverContext = driverContext;
    this.pool = new IntSingleArrayPool(driverContext.breaker());
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock valBlock = (BytesRefBlock) val.eval(page)) {
      BytesRefVector valVector = valBlock.asVector();
      if (valVector == null) {
        return eval(page.getPositionCount(), valBlock);
      }
      return eval(page.getPositionCount(), valVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += val.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, BytesRefBlock valBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      BytesRef valScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (valBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (valBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        BytesRef val = valBlock.getBytesRef(valBlock.getFirstValueIndex(p), valScratch);
        try {
          result.appendInt(BitLength.process(val));
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public IntBlock eval(int positionCount, BytesRefVector valVector) {
    final IntSingleArrayPool pool = this.pool;
    final int[] results = pool.getOrAllocate(positionCount);
    BitSet nulls = null;
    BytesRef valScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      BytesRef val = valVector.getBytesRef(p, valScratch);
      try {
        results[p] = BitLength.process(val);
      } catch (ArithmeticException e) {
        warnings().registerException(e);
        if (nulls == null) {
          nulls = new BitSet(positionCount);
        }
        nulls.set(p);
      }
    }
    if (nulls != null) {
      IntBlock block = driverContext.blockFactory().newIntArrayBlock(results, positionCount, null, nulls, Block.MvOrdering.UNORDERED);
      block.attachReleasable(() -> pool.put(results));
      return block;
    }
    IntVector result = driverContext.blockFactory().newIntArrayVector(results, positionCount);
    result.attachReleasable(() -> pool.put(results));
    return result.asBlock();
  }

  @Override
  public String toString() {
    return "BitLengthEvaluator[" + "val=" + val + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val);
    Releasables.closeExpectNoException(pool);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory val;

    public Factory(Source source, ExpressionEvaluator.Factory val) {
      this.source = source;
      this.val = val;
    }

    @Override
    public BitLengthEvaluator get(DriverContext context) {
      return new BitLengthEvaluator(source, val.get(context), context);
    }

    @Override
    public String toString() {
      return "BitLengthEvaluator[" + "val=" + val + "]";
    }
  }
}
