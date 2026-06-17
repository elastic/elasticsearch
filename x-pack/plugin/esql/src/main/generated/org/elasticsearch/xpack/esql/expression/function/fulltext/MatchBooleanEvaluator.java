// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.fulltext;

import java.lang.Boolean;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Match}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class MatchBooleanEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MatchBooleanEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator fieldBlock;

  private final Boolean query;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MatchBooleanEvaluator(Source source, ExpressionEvaluator fieldBlock, Boolean query,
      DriverContext driverContext) {
    this.source = source;
    this.fieldBlock = fieldBlock;
    this.query = query;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BooleanBlock fieldBlockBlock = (BooleanBlock) fieldBlock.eval(page)) {
      return eval(page.getPositionCount(), fieldBlockBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += fieldBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BooleanBlock fieldBlockBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(Match.processBoolean(p, fieldBlockBlock, this.query));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MatchBooleanEvaluator[" + "fieldBlock=" + fieldBlock + ", query=" + query + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(fieldBlock);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory fieldBlock;

    private final Boolean query;

    public Factory(Source source, ExpressionEvaluator.Factory fieldBlock, Boolean query) {
      this.source = source;
      this.fieldBlock = fieldBlock;
      this.query = query;
    }

    @Override
    public MatchBooleanEvaluator get(DriverContext context) {
      return new MatchBooleanEvaluator(source, fieldBlock.get(context), query, context);
    }

    @Override
    public String toString() {
      return "MatchBooleanEvaluator[" + "fieldBlock=" + fieldBlock + ", query=" + query + "]";
    }
  }
}
