// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.fulltext;

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
import org.elasticsearch.compute.data.DoubleBlock;
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
public final class MatchScoreTokenStreamEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MatchScoreTokenStreamEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator fieldBlock;

  private final Analyzer analyzer;

  private final Map<BytesRef, Integer> queryTerms;

  private final int totalWeight;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MatchScoreTokenStreamEvaluator(Source source, ExpressionEvaluator fieldBlock,
      Analyzer analyzer, Map<BytesRef, Integer> queryTerms, int totalWeight, BytesRef scratch,
      DriverContext driverContext) {
    this.source = source;
    this.fieldBlock = fieldBlock;
    this.analyzer = analyzer;
    this.queryTerms = queryTerms;
    this.totalWeight = totalWeight;
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

  public DoubleBlock eval(int positionCount, BytesRefBlock fieldBlockBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(Match.scoreTokenStream(p, fieldBlockBlock, this.analyzer, this.queryTerms, this.totalWeight, this.scratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MatchScoreTokenStreamEvaluator[" + "fieldBlock=" + fieldBlock + ", analyzer=" + analyzer + ", queryTerms=" + queryTerms + ", totalWeight=" + totalWeight + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(fieldBlock);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory fieldBlock;

    private final Analyzer analyzer;

    private final Map<BytesRef, Integer> queryTerms;

    private final int totalWeight;

    private final Function<DriverContext, BytesRef> scratch;

    public Factory(Source source, ExpressionEvaluator.Factory fieldBlock, Analyzer analyzer,
        Map<BytesRef, Integer> queryTerms, int totalWeight,
        Function<DriverContext, BytesRef> scratch) {
      this.source = source;
      this.fieldBlock = fieldBlock;
      this.analyzer = analyzer;
      this.queryTerms = queryTerms;
      this.totalWeight = totalWeight;
      this.scratch = scratch;
    }

    @Override
    public MatchScoreTokenStreamEvaluator get(DriverContext context) {
      return new MatchScoreTokenStreamEvaluator(source, fieldBlock.get(context), analyzer, queryTerms, totalWeight, scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "MatchScoreTokenStreamEvaluator[" + "fieldBlock=" + fieldBlock + ", analyzer=" + analyzer + ", queryTerms=" + queryTerms + ", totalWeight=" + totalWeight + "]";
    }
  }
}
