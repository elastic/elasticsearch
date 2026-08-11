// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.fulltext;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link RuntimeSearch}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RuntimeSearchScoreFromBooleanEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RuntimeSearchScoreFromBooleanEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator matches;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RuntimeSearchScoreFromBooleanEvaluator(Source source, ExpressionEvaluator matches,
      DriverContext driverContext) {
    this.source = source;
    this.matches = matches;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BooleanBlock matchesBlock = (BooleanBlock) matches.eval(page)) {
      return eval(page.getPositionCount(), matchesBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += matches.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, BooleanBlock matchesBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(RuntimeSearch.scoreFromBoolean(p, matchesBlock));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RuntimeSearchScoreFromBooleanEvaluator[" + "matches=" + matches + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(matches);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory matches;

    public Factory(Source source, ExpressionEvaluator.Factory matches) {
      this.source = source;
      this.matches = matches;
    }

    @Override
    public RuntimeSearchScoreFromBooleanEvaluator get(DriverContext context) {
      return new RuntimeSearchScoreFromBooleanEvaluator(source, matches.get(context), context);
    }

    @Override
    public String toString() {
      return "RuntimeSearchScoreFromBooleanEvaluator[" + "matches=" + matches + "]";
    }
  }
}
