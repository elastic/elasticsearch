// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.common.chunks.MemoryIndexChunkScorer;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link TopSnippets}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class TopSnippetsEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TopSnippetsEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator field;

  private final EvalOperator.ExpressionEvaluator query;

  private final ChunkingSettings chunkingSettings;

  private final MemoryIndexChunkScorer scorer;

  private final int numSnippets;

  private final DriverContext driverContext;

  private Warnings warnings;

  public TopSnippetsEvaluator(Source source, EvalOperator.ExpressionEvaluator field,
      EvalOperator.ExpressionEvaluator query, ChunkingSettings chunkingSettings,
      MemoryIndexChunkScorer scorer, int numSnippets, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.query = query;
    this.chunkingSettings = chunkingSettings;
    this.scorer = scorer;
    this.numSnippets = numSnippets;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock fieldBlock = (BytesRefBlock) field.eval(page)) {
      try (BytesRefBlock queryBlock = (BytesRefBlock) query.eval(page)) {
        return eval(page.getPositionCount(), fieldBlock, queryBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    baseRamBytesUsed += query.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock fieldBlock, BytesRefBlock queryBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!fieldBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!queryBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          TopSnippets.process(result, p, fieldBlock, queryBlock, this.chunkingSettings, this.scorer, this.numSnippets);
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "TopSnippetsEvaluator[" + "field=" + field + ", query=" + query + ", chunkingSettings=" + chunkingSettings + ", scorer=" + scorer + ", numSnippets=" + numSnippets + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field, query);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory field;

    private final EvalOperator.ExpressionEvaluator.Factory query;

    private final ChunkingSettings chunkingSettings;

    private final MemoryIndexChunkScorer scorer;

    private final int numSnippets;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field,
        EvalOperator.ExpressionEvaluator.Factory query, ChunkingSettings chunkingSettings,
        MemoryIndexChunkScorer scorer, int numSnippets) {
      this.source = source;
      this.field = field;
      this.query = query;
      this.chunkingSettings = chunkingSettings;
      this.scorer = scorer;
      this.numSnippets = numSnippets;
    }

    @Override
    public TopSnippetsEvaluator get(DriverContext context) {
      return new TopSnippetsEvaluator(source, field.get(context), query.get(context), chunkingSettings, scorer, numSnippets, context);
    }

    @Override
    public String toString() {
      return "TopSnippetsEvaluator[" + "field=" + field + ", query=" + query + ", chunkingSettings=" + chunkingSettings + ", scorer=" + scorer + ", numSnippets=" + numSnippets + "]";
    }
  }
}
