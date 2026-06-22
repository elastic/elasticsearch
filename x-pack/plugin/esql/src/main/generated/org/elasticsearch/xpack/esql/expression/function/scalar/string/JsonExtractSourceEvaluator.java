// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link JsonExtract}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class JsonExtractSourceEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(JsonExtractSourceEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator strBlock;

  private final ExpressionEvaluator pathBlock;

  private final DriverContext driverContext;

  private Warnings warnings;

  public JsonExtractSourceEvaluator(Source source, ExpressionEvaluator strBlock,
      ExpressionEvaluator pathBlock, DriverContext driverContext) {
    this.source = source;
    this.strBlock = strBlock;
    this.pathBlock = pathBlock;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlockBlock = (BytesRefBlock) strBlock.eval(page)) {
      try (BytesRefBlock pathBlockBlock = (BytesRefBlock) pathBlock.eval(page)) {
        return eval(page.getPositionCount(), strBlockBlock, pathBlockBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += strBlock.baseRamBytesUsed();
    baseRamBytesUsed += pathBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlockBlock,
      BytesRefBlock pathBlockBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          JsonExtract.processSource(result, p, strBlockBlock, pathBlockBlock);
        } catch (IllegalArgumentException | IllegalStateException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "JsonExtractSourceEvaluator[" + "strBlock=" + strBlock + ", pathBlock=" + pathBlock + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(strBlock, pathBlock);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory strBlock;

    private final ExpressionEvaluator.Factory pathBlock;

    public Factory(Source source, ExpressionEvaluator.Factory strBlock,
        ExpressionEvaluator.Factory pathBlock) {
      this.source = source;
      this.strBlock = strBlock;
      this.pathBlock = pathBlock;
    }

    @Override
    public JsonExtractSourceEvaluator get(DriverContext context) {
      return new JsonExtractSourceEvaluator(source, strBlock.get(context), pathBlock.get(context), context);
    }

    @Override
    public String toString() {
      return "JsonExtractSourceEvaluator[" + "strBlock=" + strBlock + ", pathBlock=" + pathBlock + "]";
    }
  }
}
