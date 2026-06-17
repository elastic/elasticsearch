// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.fulltext;

import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
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
public final class MatchBytesRefEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MatchBytesRefEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator fieldBlock;

  private final BytesRef queryStringBytesRef;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MatchBytesRefEvaluator(Source source, ExpressionEvaluator fieldBlock,
      BytesRef queryStringBytesRef, BytesRef scratch, DriverContext driverContext) {
    this.source = source;
    this.fieldBlock = fieldBlock;
    this.queryStringBytesRef = queryStringBytesRef;
    this.scratch = scratch;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock fieldBlockBlock = (BytesRefBlock) fieldBlock.eval(page)) {
      return eval(page.getPositionCount(), fieldBlockBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += fieldBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock fieldBlockBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(Match.processBytesRef(p, fieldBlockBlock, this.queryStringBytesRef, this.scratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MatchBytesRefEvaluator[" + "fieldBlock=" + fieldBlock + ", queryStringBytesRef=" + queryStringBytesRef + "]";
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

    private final BytesRef queryStringBytesRef;

    private final Function<DriverContext, BytesRef> scratch;

    public Factory(Source source, ExpressionEvaluator.Factory fieldBlock,
        BytesRef queryStringBytesRef, Function<DriverContext, BytesRef> scratch) {
      this.source = source;
      this.fieldBlock = fieldBlock;
      this.queryStringBytesRef = queryStringBytesRef;
      this.scratch = scratch;
    }

    @Override
    public MatchBytesRefEvaluator get(DriverContext context) {
      return new MatchBytesRefEvaluator(source, fieldBlock.get(context), queryStringBytesRef, scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "MatchBytesRefEvaluator[" + "fieldBlock=" + fieldBlock + ", queryStringBytesRef=" + queryStringBytesRef + "]";
    }
  }
}
