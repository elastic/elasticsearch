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
public final class JsonExtractSourceConstantEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(JsonExtractSourceConstantEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator strBlock;

  private final JsonPath path;

  private final DriverContext driverContext;

  private Warnings warnings;

  public JsonExtractSourceConstantEvaluator(Source source, ExpressionEvaluator strBlock,
      JsonPath path, DriverContext driverContext) {
    this.source = source;
    this.strBlock = strBlock;
    this.path = path;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlockBlock = (BytesRefBlock) strBlock.eval(page)) {
      return eval(page.getPositionCount(), strBlockBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += strBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlockBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          JsonExtract.processSourceConstant(result, p, strBlockBlock, this.path);
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
    return "JsonExtractSourceConstantEvaluator[" + "strBlock=" + strBlock + ", path=" + path + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(strBlock);
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

    private final JsonPath path;

    public Factory(Source source, ExpressionEvaluator.Factory strBlock, JsonPath path) {
      this.source = source;
      this.strBlock = strBlock;
      this.path = path;
    }

    @Override
    public JsonExtractSourceConstantEvaluator get(DriverContext context) {
      return new JsonExtractSourceConstantEvaluator(source, strBlock.get(context), path, context);
    }

    @Override
    public String toString() {
      return "JsonExtractSourceConstantEvaluator[" + "strBlock=" + strBlock + ", path=" + path + "]";
    }
  }
}
