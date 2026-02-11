// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

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
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Chunk}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ChunkEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ChunkEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator field;

  private final ChunkingSettings chunkingSettings;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ChunkEvaluator(Source source, EvalOperator.ExpressionEvaluator field,
      ChunkingSettings chunkingSettings, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.chunkingSettings = chunkingSettings;
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
        Chunk.process(result, p, fieldBlock, this.chunkingSettings);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ChunkEvaluator[" + "field=" + field + ", chunkingSettings=" + chunkingSettings + "]";
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

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory field;

    private final ChunkingSettings chunkingSettings;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field,
        ChunkingSettings chunkingSettings) {
      this.source = source;
      this.field = field;
      this.chunkingSettings = chunkingSettings;
    }

    @Override
    public ChunkEvaluator get(DriverContext context) {
      return new ChunkEvaluator(source, field.get(context), chunkingSettings, context);
    }

    @Override
    public String toString() {
      return "ChunkEvaluator[" + "field=" + field + ", chunkingSettings=" + chunkingSettings + "]";
    }
  }
}
