// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongSingleArrayPool;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Now}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class NowEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(NowEvaluator.class);

  private final Source source;

  private final long now;

  private final DriverContext driverContext;

  private Warnings warnings;

  private final LongSingleArrayPool pool;

  public NowEvaluator(Source source, long now, DriverContext driverContext) {
    this.source = source;
    this.now = now;
    this.driverContext = driverContext;
    this.pool = new LongSingleArrayPool(driverContext.breaker());
  }

  @Override
  public Block eval(Page page) {
    return eval(page.getPositionCount()).asBlock();
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    return baseRamBytesUsed;
  }

  public LongVector eval(int positionCount) {
    final LongSingleArrayPool pool = this.pool;
    final long[] results = pool.getOrAllocate(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      results[p] = Now.process(this.now);
    }
    LongVector result = driverContext.blockFactory().newLongArrayVector(results, positionCount);
    result.attachReleasable(() -> pool.put(results));
    return result;
  }

  @Override
  public String toString() {
    return "NowEvaluator[" + "now=" + now + "]";
  }

  @Override
  public void close() {
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

    private final long now;

    public Factory(Source source, long now) {
      this.source = source;
      this.now = now;
    }

    @Override
    public NowEvaluator get(DriverContext context) {
      return new NowEvaluator(source, now, context);
    }

    @Override
    public String toString() {
      return "NowEvaluator[" + "now=" + now + "]";
    }
  }
}
