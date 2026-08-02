// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.promql;

import com.google.re2j.Pattern;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
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
 * {@link ExpressionEvaluator} implementation for {@link PromqlRegexExtract}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class PromqlRegexExtractEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PromqlRegexExtractEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator srcBlock;

  private final Pattern pattern;

  private final BytesRef replacement;

  private final DriverContext driverContext;

  private Warnings warnings;

  public PromqlRegexExtractEvaluator(Source source, ExpressionEvaluator srcBlock, Pattern pattern,
      BytesRef replacement, DriverContext driverContext) {
    this.source = source;
    this.srcBlock = srcBlock;
    this.pattern = pattern;
    this.replacement = replacement;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock srcBlockBlock = (BytesRefBlock) srcBlock.eval(page)) {
      return eval(page.getPositionCount(), srcBlockBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += srcBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock srcBlockBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!srcBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        PromqlRegexExtract.process(result, p, srcBlockBlock, this.pattern, this.replacement);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "PromqlRegexExtractEvaluator[" + "srcBlock=" + srcBlock + ", pattern=" + pattern + ", replacement=" + replacement + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(srcBlock);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory srcBlock;

    private final Pattern pattern;

    private final BytesRef replacement;

    public Factory(Source source, ExpressionEvaluator.Factory srcBlock, Pattern pattern,
        BytesRef replacement) {
      this.source = source;
      this.srcBlock = srcBlock;
      this.pattern = pattern;
      this.replacement = replacement;
    }

    @Override
    public PromqlRegexExtractEvaluator get(DriverContext context) {
      return new PromqlRegexExtractEvaluator(source, srcBlock.get(context), pattern, replacement, context);
    }

    @Override
    public String toString() {
      return "PromqlRegexExtractEvaluator[" + "srcBlock=" + srcBlock + ", pattern=" + pattern + ", replacement=" + replacement + "]";
    }
  }
}
