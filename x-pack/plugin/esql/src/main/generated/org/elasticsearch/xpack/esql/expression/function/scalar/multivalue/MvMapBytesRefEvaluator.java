// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * {@link ExpressionEvaluator} implementation for {@link MvMap}.
 * This class is generated. Edit {@code LambdaEvaluatorImplementer} instead.
 */
public final class MvMapBytesRefEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvMapBytesRefEvaluator.class);

  private final ExpressionEvaluator field;

  private final ExpressionEvaluator lambda;

  private final int[] outerChannels;

  private final DriverContext driverContext;

  public MvMapBytesRefEvaluator(ExpressionEvaluator field, ExpressionEvaluator lambda,
      int[] outerChannels, DriverContext driverContext) {
    this.field = field;
    this.lambda = lambda;
    this.outerChannels = outerChannels;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (Block fieldBlock = field.eval(page)) {
      if (fieldBlock.areAllValuesNull()) {
        return driverContext.blockFactory().newConstantNullBlock(page.getPositionCount());
      }
      if (fieldBlock.mayHaveMultivaluedFields() == false && fieldBlock.mayHaveNulls() == false) {
        return evalNotExpanded(page, fieldBlock);
      }
      return evalExpanded(page, fieldBlock);
    }
  }

  /**
   * Fast path: every field position holds exactly one non-null value, so the lambda body
   * is evaluated over the page's own shape with the field itself as the parameter block.
   */
  private Block evalNotExpanded(Page page, Block fieldBlock) {
    Block[] inner = new Block[outerChannels.length + 1];
    Page innerPage = null;
    try {
      for (int c = 0; c < outerChannels.length; c++) {
        Block b = page.getBlock(outerChannels[c]);
        b.incRef();
        inner[c] = b;
      }
      fieldBlock.incRef();
      inner[outerChannels.length] = fieldBlock;
      innerPage = new Page(inner);
      try (BytesRefBlock lambdaBlock = (BytesRefBlock) lambda.eval(innerPage); BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(fieldBlock.getPositionCount())) {
        for (int p = 0; p < fieldBlock.getPositionCount(); p++) {
          MvMap.process(result, lambdaBlock, p, p + 1);
        }
        return result.build();
      }
    } finally {
      if (innerPage != null) {
        innerPage.releaseBlocks();
      } else {
        Releasables.closeExpectNoException(inner);
      }
    }
  }

  /**
   * Expanded path: multivalued field positions are flattened into one row per value (null
   * positions become a single null row) and the lambda body is evaluated once over the
   * flattened shape. The combine method is invoked with each original position's row range;
   * null field positions produce a null result without invoking it.
   */
  private Block evalExpanded(Page page, Block fieldBlock) {
    Block[] inner = new Block[outerChannels.length + 1];
    Page innerPage = null;
    try {
      if (outerChannels.length > 0) {
        int[] expandingFilter = expandingFilter(fieldBlock);
        for (int c = 0; c < outerChannels.length; c++) {
          inner[c] = page.getBlock(outerChannels[c]).filter(true, expandingFilter);
        }
      }
      inner[outerChannels.length] = fieldBlock.expand();
      innerPage = new Page(inner);
      try (BytesRefBlock lambdaBlock = (BytesRefBlock) lambda.eval(innerPage); BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(fieldBlock.getPositionCount())) {
        int row = 0;
        for (int p = 0; p < fieldBlock.getPositionCount(); p++) {
          int valueCount = fieldBlock.getValueCount(p);
          if (valueCount == 0) {
            result.appendNull();
            // null field positions expand to a single null row
            row++;
            continue;
          }
          MvMap.process(result, lambdaBlock, row, row + valueCount);
          row += valueCount;
        }
        return result.build();
      }
    } finally {
      if (innerPage != null) {
        innerPage.releaseBlocks();
      } else {
        Releasables.closeExpectNoException(inner);
      }
    }
  }

  /**
   * Maps each row of the flattened field to the original position it came from, used to
   * row-replicate the upstream blocks the lambda body references. Null field positions
   * occupy a single row, mirroring {@link Block#expand}.
   */
  private static int[] expandingFilter(Block fieldBlock) {
    int rows = 0;
    for (int p = 0; p < fieldBlock.getPositionCount(); p++) {
      int valueCount = fieldBlock.getValueCount(p);
      rows += valueCount == 0 ? 1 : valueCount;
    }
    int[] expandingFilter = new int[rows];
    int row = 0;
    for (int p = 0; p < fieldBlock.getPositionCount(); p++) {
      int valueCount = fieldBlock.getValueCount(p);
      if (valueCount == 0) {
        valueCount = 1;
      }
      Arrays.fill(expandingFilter, row, row + valueCount, p);
      row += valueCount;
    }
    return expandingFilter;
  }

  @Override
  public long baseRamBytesUsed() {
    return BASE_RAM_BYTES_USED + field.baseRamBytesUsed() + lambda.baseRamBytesUsed();
  }

  @Override
  public String toString() {
    return "MvMapBytesRefEvaluator[field=" + field + ", lambda=" + lambda + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field, lambda);
  }

  public static class Factory implements ExpressionEvaluator.Factory {
    private final ExpressionEvaluator.Factory field;

    private final ExpressionEvaluator.Factory lambda;

    private final int[] outerChannels;

    public Factory(ExpressionEvaluator.Factory field, ExpressionEvaluator.Factory lambda,
        int[] outerChannels) {
      this.field = field;
      this.lambda = lambda;
      this.outerChannels = outerChannels;
    }

    @Override
    public MvMapBytesRefEvaluator get(DriverContext context) {
      return new MvMapBytesRefEvaluator(field.get(context), lambda.get(context), outerChannels, context);
    }

    @Override
    public String toString() {
      return "MvMapBytesRefEvaluator[field=" + field + ", lambda=" + lambda + "]";
    }
  }
}
