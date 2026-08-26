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
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link FmtBytes}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class FmtBytesFromIntWithUnitEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FmtBytesFromIntWithUnitEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator bytes;

  private final ExpressionEvaluator unit;

  private final DriverContext driverContext;

  private Warnings warnings;

  public FmtBytesFromIntWithUnitEvaluator(Source source, ExpressionEvaluator bytes,
      ExpressionEvaluator unit, DriverContext driverContext) {
    this.source = source;
    this.bytes = bytes;
    this.unit = unit;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock bytesBlock = (IntBlock) bytes.eval(page)) {
      try (BytesRefBlock unitBlock = (BytesRefBlock) unit.eval(page)) {
        IntVector bytesVector = bytesBlock.asVector();
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

  public BytesRefBlock eval(int positionCount, IntBlock bytesBlock, BytesRefBlock unitBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef unitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (bytesBlock.getValueCount(p)) {
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
        switch (unitBlock.getValueCount(p)) {
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
        int bytes = bytesBlock.getInt(bytesBlock.getFirstValueIndex(p));
        BytesRef unit = unitBlock.getBytesRef(unitBlock.getFirstValueIndex(p), unitScratch);
        try {
          result.appendBytesRef(FmtBytes.processIntWithUnit(bytes, unit));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, IntVector bytesVector, BytesRefVector unitVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef unitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        int bytes = bytesVector.getInt(p);
        BytesRef unit = unitVector.getBytesRef(p, unitScratch);
        try {
          result.appendBytesRef(FmtBytes.processIntWithUnit(bytes, unit));
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
    return "FmtBytesFromIntWithUnitEvaluator[" + "bytes=" + bytes + ", unit=" + unit + "]";
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
    public FmtBytesFromIntWithUnitEvaluator get(DriverContext context) {
      return new FmtBytesFromIntWithUnitEvaluator(source, bytes.get(context), unit.get(context), context);
    }

    @Override
    public String toString() {
      return "FmtBytesFromIntWithUnitEvaluator[" + "bytes=" + bytes + ", unit=" + unit + "]";
    }
  }
}
