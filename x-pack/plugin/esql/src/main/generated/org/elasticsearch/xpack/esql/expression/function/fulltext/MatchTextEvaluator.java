// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.fulltext;

import java.io.IOException;
import java.lang.Override;
import java.lang.String;
import java.util.Set;
import java.util.function.Function;
import org.apache.lucene.analysis.Analyzer;
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
public final class MatchTextEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MatchTextEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator fieldBlock;

  private final Set<BytesRef> queryTerms;

  private final Analyzer analyzer;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MatchTextEvaluator(Source source, ExpressionEvaluator fieldBlock, Set<BytesRef> queryTerms,
      Analyzer analyzer, BytesRef scratch, DriverContext driverContext) {
    this.source = source;
    this.fieldBlock = fieldBlock;
    this.queryTerms = queryTerms;
    this.analyzer = analyzer;
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
        try {
          result.appendBoolean(Match.processText(p, fieldBlockBlock, this.queryTerms, this.analyzer, this.scratch));
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
    return "MatchTextEvaluator[" + "fieldBlock=" + fieldBlock + ", queryTerms=" + queryTerms + ", analyzer=" + analyzer + "]";
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

    private final Set<BytesRef> queryTerms;

    private final Analyzer analyzer;

    private final Function<DriverContext, BytesRef> scratch;

    public Factory(Source source, ExpressionEvaluator.Factory fieldBlock, Set<BytesRef> queryTerms,
        Analyzer analyzer, Function<DriverContext, BytesRef> scratch) {
      this.source = source;
      this.fieldBlock = fieldBlock;
      this.queryTerms = queryTerms;
      this.analyzer = analyzer;
      this.scratch = scratch;
    }

    @Override
    public MatchTextEvaluator get(DriverContext context) {
      return new MatchTextEvaluator(source, fieldBlock.get(context), queryTerms, analyzer, scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "MatchTextEvaluator[" + "fieldBlock=" + fieldBlock + ", queryTerms=" + queryTerms + ", analyzer=" + analyzer + "]";
    }
  }
}
