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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link FmtBytesSi}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class FmtBytesSiFromLongWithUnitEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FmtBytesSiFromLongWithUnitEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator bytes;

  private final ExpressionEvaluator unit;

  private final DriverContext driverContext;

  private Warnings warnings;

  public FmtBytesSiFromLongWithUnitEvaluator(Source source, ExpressionEvaluator bytes,
      ExpressionEvaluator unit, DriverContext driverContext) {
    this.source = source;
    this.bytes = bytes;
    this.unit = unit;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock bytesBlock = (LongBlock) bytes.eval(page)) {
      try (BytesRefBlock unitBlock = (BytesRefBlock) unit.eval(page)) {
        LongVector bytesVector = bytesBlock.asVector();
        if (bytesVector == null) {
          return eval(page.getPositionCount(), bytesBlock, unitBlock);
        }
        BytesRefVector unitVector = unitBlock.asVector();
        if (unitVector == null) {
          return eval(page.getPositionCount(), bytesBlock, unitBlock);
        }
        return eval(page.getPositionCount(), bytesVector, unitVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += bytes.baseRamBytesUsed();
    baseRamBytesUsed += unit.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, LongBlock bytesBlock, BytesRefBlock unitBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef unitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (bytesBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (bytesBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        if (unitBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (unitBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        long bytes = bytesBlock.getLong(bytesBlock.getFirstValueIndex(p));
        BytesRef unit = unitBlock.getBytesRef(unitBlock.getFirstValueIndex(p), unitScratch);
        try {
          result.appendBytesRef(FmtBytesSi.processLongWithUnit(bytes, unit));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, LongVector bytesVector, BytesRefVector unitVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef unitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        long bytes = bytesVector.getLong(p);
        BytesRef unit = unitVector.getBytesRef(p, unitScratch);
        try {
          result.appendBytesRef(FmtBytesSi.processLongWithUnit(bytes, unit));
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
    return "FmtBytesSiFromLongWithUnitEvaluator[" + "bytes=" + bytes + ", unit=" + unit + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(bytes, unit);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory bytes;

    private final ExpressionEvaluator.Factory unit;

    public Factory(Source source, ExpressionEvaluator.Factory bytes,
        ExpressionEvaluator.Factory unit) {
      this.source = source;
      this.bytes = bytes;
      this.unit = unit;
    }

    @Override
    public FmtBytesSiFromLongWithUnitEvaluator get(DriverContext context) {
      return new FmtBytesSiFromLongWithUnitEvaluator(source, bytes.get(context), unit.get(context), context);
    }

    @Override
    public String toString() {
      return "FmtBytesSiFromLongWithUnitEvaluator[" + "bytes=" + bytes + ", unit=" + unit + "]";
    }
  }
}
