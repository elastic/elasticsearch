// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link TimeSeriesUnset}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class TimeSeriesUnsetEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TimeSeriesUnsetEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator timeseries;

  private final TimeSeriesMetadataJson.Unset unset;

  private final DriverContext driverContext;

  private Warnings warnings;

  public TimeSeriesUnsetEvaluator(Source source, ExpressionEvaluator timeseries,
      TimeSeriesMetadataJson.Unset unset, DriverContext driverContext) {
    this.source = source;
    this.timeseries = timeseries;
    this.unset = unset;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock timeseriesBlock = (BytesRefBlock) timeseries.eval(page)) {
      BytesRefVector timeseriesVector = timeseriesBlock.asVector();
      if (timeseriesVector == null) {
        return eval(page.getPositionCount(), timeseriesBlock);
      }
      return eval(page.getPositionCount(), timeseriesVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += timeseries.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock timeseriesBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef timeseriesScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (timeseriesBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (timeseriesBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        BytesRef timeseries = timeseriesBlock.getBytesRef(timeseriesBlock.getFirstValueIndex(p), timeseriesScratch);
        try {
          result.appendBytesRef(TimeSeriesUnset.process(timeseries, this.unset));
        } catch (IllegalArgumentException | IOException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector timeseriesVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef timeseriesScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef timeseries = timeseriesVector.getBytesRef(p, timeseriesScratch);
        try {
          result.appendBytesRef(TimeSeriesUnset.process(timeseries, this.unset));
        } catch (IllegalArgumentException | IOException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "TimeSeriesUnsetEvaluator[" + "timeseries=" + timeseries + ", unset=" + unset + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(timeseries);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory timeseries;

    private final TimeSeriesMetadataJson.Unset unset;

    public Factory(Source source, ExpressionEvaluator.Factory timeseries,
        TimeSeriesMetadataJson.Unset unset) {
      this.source = source;
      this.timeseries = timeseries;
      this.unset = unset;
    }

    @Override
    public TimeSeriesUnsetEvaluator get(DriverContext context) {
      return new TimeSeriesUnsetEvaluator(source, timeseries.get(context), unset, context);
    }

    @Override
    public String toString() {
      return "TimeSeriesUnsetEvaluator[" + "timeseries=" + timeseries + ", unset=" + unset + "]";
    }
  }
}
