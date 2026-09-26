// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link PackDimValues}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class PackDimValuesUnsetEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PackDimValuesUnsetEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator record;

  private final BytesRef[] removed;

  private final DriverContext driverContext;

  private Warnings warnings;

  public PackDimValuesUnsetEvaluator(Source source, ExpressionEvaluator record, BytesRef[] removed,
      DriverContext driverContext) {
    this.source = source;
    this.record = record;
    this.removed = removed;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (PackDimBlock recordBlock = (PackDimBlock) record.eval(page)) {
      return eval(page.getPositionCount(), recordBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += record.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public PackDimBlock eval(int positionCount, PackDimBlock recordBlock) {
    try(PackDimBlock.Builder result = driverContext.blockFactory().newPackDimBlockBuilder(positionCount)) {
      PackDimValue recordScratch = new PackDimValue();
      position: for (int p = 0; p < positionCount; p++) {
        if (recordBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (recordBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        PackDimValue record = recordBlock.getPackDim(recordBlock.getFirstValueIndex(p), recordScratch);
        PackDimValues.unset(result, record, this.removed);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "PackDimValuesUnsetEvaluator[" + "record=" + record + ", removed=" + removed + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(record);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory record;

    private final BytesRef[] removed;

    public Factory(Source source, ExpressionEvaluator.Factory record, BytesRef[] removed) {
      this.source = source;
      this.record = record;
      this.removed = removed;
    }

    @Override
    public PackDimValuesUnsetEvaluator get(DriverContext context) {
      return new PackDimValuesUnsetEvaluator(source, record.get(context), removed, context);
    }

    @Override
    public String toString() {
      return "PackDimValuesUnsetEvaluator[" + "record=" + record + ", removed=" + removed + "]";
    }
  }
}
