// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
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
public final class ChunkBytesRefEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ChunkBytesRefEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator numChunks;

  private final ChunkingSettings chunkingSettings;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ChunkBytesRefEvaluator(Source source, EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator numChunks, ChunkingSettings chunkingSettings,
      DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.numChunks = numChunks;
    this.chunkingSettings = chunkingSettings;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (IntBlock numChunksBlock = (IntBlock) numChunks.eval(page)) {
        BytesRefVector strVector = strBlock.asVector();
        if (strVector == null) {
          return eval(page.getPositionCount(), strBlock, numChunksBlock);
        }
        IntVector numChunksVector = numChunksBlock.asVector();
        if (numChunksVector == null) {
          return eval(page.getPositionCount(), strBlock, numChunksBlock);
        }
        return eval(page.getPositionCount(), strVector, numChunksVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += str.baseRamBytesUsed();
    baseRamBytesUsed += numChunks.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, IntBlock numChunksBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (strBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        switch (numChunksBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        BytesRef str = strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch);
        int numChunks = numChunksBlock.getInt(numChunksBlock.getFirstValueIndex(p));
        Chunk.process(result, str, numChunks, this.chunkingSettings);
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector strVector,
      IntVector numChunksVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef str = strVector.getBytesRef(p, strScratch);
        int numChunks = numChunksVector.getInt(p);
        Chunk.process(result, str, numChunks, this.chunkingSettings);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ChunkBytesRefEvaluator[" + "str=" + str + ", numChunks=" + numChunks + ", chunkingSettings=" + chunkingSettings + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, numChunks);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(
              driverContext.warningsMode(),
              source.source().getLineNumber(),
              source.source().getColumnNumber(),
              source.text()
          );
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory str;

    private final EvalOperator.ExpressionEvaluator.Factory numChunks;

    private final ChunkingSettings chunkingSettings;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory numChunks, ChunkingSettings chunkingSettings) {
      this.source = source;
      this.str = str;
      this.numChunks = numChunks;
      this.chunkingSettings = chunkingSettings;
    }

    @Override
    public ChunkBytesRefEvaluator get(DriverContext context) {
      return new ChunkBytesRefEvaluator(source, str.get(context), numChunks.get(context), chunkingSettings, context);
    }

    @Override
    public String toString() {
      return "ChunkBytesRefEvaluator[" + "str=" + str + ", numChunks=" + numChunks + ", chunkingSettings=" + chunkingSettings + "]";
    }
  }
}
