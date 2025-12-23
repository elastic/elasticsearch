// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StYMin}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StYMinFromDocValuesGeoEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StYMinFromDocValuesGeoEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator encodedBlock;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StYMinFromDocValuesGeoEvaluator(Source source,
      EvalOperator.ExpressionEvaluator encodedBlock, DriverContext driverContext) {
    this.source = source;
    this.encodedBlock = encodedBlock;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock encodedBlockBlock = (LongBlock) encodedBlock.eval(page)) {
      return eval(page.getPositionCount(), encodedBlockBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += encodedBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, LongBlock encodedBlockBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!encodedBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          StYMin.fromDocValuesGeo(result, p, encodedBlockBlock);
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
    return "StYMinFromDocValuesGeoEvaluator[" + "encodedBlock=" + encodedBlock + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(encodedBlock);
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

    private final EvalOperator.ExpressionEvaluator.Factory encodedBlock;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory encodedBlock) {
      this.source = source;
      this.encodedBlock = encodedBlock;
    }

    @Override
    public StYMinFromDocValuesGeoEvaluator get(DriverContext context) {
      return new StYMinFromDocValuesGeoEvaluator(source, encodedBlock.get(context), context);
    }

    @Override
    public String toString() {
      return "StYMinFromDocValuesGeoEvaluator[" + "encodedBlock=" + encodedBlock + "]";
    }
  }
}
