// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.fulltext;

import java.io.IOException;
import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.Map;
import java.util.function.Function;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link RuntimeBm25RowStats}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RuntimeBm25RowStatsEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RuntimeBm25RowStatsEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator field;

  private final Analyzer analyzer;

  private final Map<BytesRef, Integer> termIndex;

  private final boolean[] present;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RuntimeBm25RowStatsEvaluator(Source source, ExpressionEvaluator field, Analyzer analyzer,
      Map<BytesRef, Integer> termIndex, boolean[] present, BytesRef scratch,
      DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.analyzer = analyzer;
    this.termIndex = termIndex;
    this.present = present;
    this.scratch = scratch;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock fieldBlock = (BytesRefBlock) field.eval(page)) {
      return eval(page.getPositionCount(), fieldBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, BytesRefBlock fieldBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!fieldBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          RuntimeBm25RowStats.process(result, p, fieldBlock, this.analyzer, this.termIndex, this.present, this.scratch);
        } catch (IOException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RuntimeBm25RowStatsEvaluator[" + "field=" + field + ", analyzer=" + analyzer + ", termIndex=" + termIndex + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory field;

    private final Analyzer analyzer;

    private final Map<BytesRef, Integer> termIndex;

    private final Function<DriverContext, boolean[]> present;

    private final Function<DriverContext, BytesRef> scratch;

    public Factory(Source source, ExpressionEvaluator.Factory field, Analyzer analyzer,
        Map<BytesRef, Integer> termIndex, Function<DriverContext, boolean[]> present,
        Function<DriverContext, BytesRef> scratch) {
      this.source = source;
      this.field = field;
      this.analyzer = analyzer;
      this.termIndex = termIndex;
      this.present = present;
      this.scratch = scratch;
    }

    @Override
    public RuntimeBm25RowStatsEvaluator get(DriverContext context) {
      return new RuntimeBm25RowStatsEvaluator(source, field.get(context), analyzer, termIndex, present.apply(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "RuntimeBm25RowStatsEvaluator[" + "field=" + field + ", analyzer=" + analyzer + ", termIndex=" + termIndex + "]";
    }
  }
}
