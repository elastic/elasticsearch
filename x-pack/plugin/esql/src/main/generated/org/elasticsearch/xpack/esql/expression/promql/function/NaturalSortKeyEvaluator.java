// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.promql.function;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link NaturalSortKey}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class NaturalSortKeyEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(NaturalSortKeyEvaluator.class);

  private final Source source;

  private final BreakingBytesRefBuilder scratch;

  private final ExpressionEvaluator v;

  private final DriverContext driverContext;

  private Warnings warnings;

  public NaturalSortKeyEvaluator(Source source, BreakingBytesRefBuilder scratch,
      ExpressionEvaluator v, DriverContext driverContext) {
    this.source = source;
    this.scratch = scratch;
    this.v = v;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock vBlock = (BytesRefBlock) v.eval(page)) {
      BytesRefVector vVector = vBlock.asVector();
      if (vVector == null) {
        return eval(page.getPositionCount(), vBlock);
      }
      return eval(page.getPositionCount(), vVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += v.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock vBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef vScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (vBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (vBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        BytesRef v = vBlock.getBytesRef(vBlock.getFirstValueIndex(p), vScratch);
        result.appendBytesRef(NaturalSortKey.process(this.scratch, v));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector vVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef vScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef v = vVector.getBytesRef(p, vScratch);
        result.appendBytesRef(NaturalSortKey.process(this.scratch, v));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "NaturalSortKeyEvaluator[" + "v=" + v + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(scratch, v);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final Function<DriverContext, BreakingBytesRefBuilder> scratch;

    private final ExpressionEvaluator.Factory v;

    public Factory(Source source, Function<DriverContext, BreakingBytesRefBuilder> scratch,
        ExpressionEvaluator.Factory v) {
      this.source = source;
      this.scratch = scratch;
      this.v = v;
    }

    @Override
    public NaturalSortKeyEvaluator get(DriverContext context) {
      return new NaturalSortKeyEvaluator(source, scratch.apply(context), v.get(context), context);
    }

    @Override
    public String toString() {
      return "NaturalSortKeyEvaluator[" + "v=" + v + "]";
    }
  }
}
