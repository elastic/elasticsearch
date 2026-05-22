// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.common.chunks.MemoryIndexChunkScorer;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link TopSnippets}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class TopSnippetsEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TopSnippetsEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator field;

  private final String queryString;

  private final ChunkingSettings chunkingSettings;

  private final MemoryIndexChunkScorer scorer;

  private final int numSnippets;

  private final boolean docOrder;

  private final PassageFormatter highlightFormatter;

  private final DriverContext driverContext;

  private Warnings warnings;

  public TopSnippetsEvaluator(Source source, ExpressionEvaluator field, String queryString,
      ChunkingSettings chunkingSettings, MemoryIndexChunkScorer scorer, int numSnippets,
      boolean docOrder, PassageFormatter highlightFormatter, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.queryString = queryString;
    this.chunkingSettings = chunkingSettings;
    this.scorer = scorer;
    this.numSnippets = numSnippets;
    this.docOrder = docOrder;
    this.highlightFormatter = highlightFormatter;
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

  public BytesRefBlock eval(int positionCount, BytesRefBlock fieldBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
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
          TopSnippets.process(result, p, fieldBlock, this.queryString, this.chunkingSettings, this.scorer, this.numSnippets, this.docOrder, this.highlightFormatter);
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
    return "TopSnippetsEvaluator[" + "field=" + field + ", queryString=" + queryString + ", chunkingSettings=" + chunkingSettings + ", scorer=" + scorer + ", numSnippets=" + numSnippets + ", docOrder=" + docOrder + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory field;

    private final String queryString;

    private final ChunkingSettings chunkingSettings;

    private final MemoryIndexChunkScorer scorer;

    private final int numSnippets;

    private final boolean docOrder;

    private final PassageFormatter highlightFormatter;

    public Factory(Source source, ExpressionEvaluator.Factory field, String queryString,
        ChunkingSettings chunkingSettings, MemoryIndexChunkScorer scorer, int numSnippets,
        boolean docOrder, PassageFormatter highlightFormatter) {
      this.source = source;
      this.field = field;
      this.queryString = queryString;
      this.chunkingSettings = chunkingSettings;
      this.scorer = scorer;
      this.numSnippets = numSnippets;
      this.docOrder = docOrder;
      this.highlightFormatter = highlightFormatter;
    }

    @Override
    public TopSnippetsEvaluator get(DriverContext context) {
      return new TopSnippetsEvaluator(source, field.get(context), queryString, chunkingSettings, scorer, numSnippets, docOrder, highlightFormatter, context);
    }

    @Override
    public String toString() {
      return "TopSnippetsEvaluator[" + "field=" + field + ", queryString=" + queryString + ", chunkingSettings=" + chunkingSettings + ", scorer=" + scorer + ", numSnippets=" + numSnippets + ", docOrder=" + docOrder + "]";
    }
  }
}
