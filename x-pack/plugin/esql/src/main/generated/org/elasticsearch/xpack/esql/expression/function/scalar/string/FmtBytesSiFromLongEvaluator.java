// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
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
public final class FmtBytesSiFromLongEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FmtBytesSiFromLongEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator bytes;

  private final DriverContext driverContext;

  private Warnings warnings;

  public FmtBytesSiFromLongEvaluator(Source source, ExpressionEvaluator bytes,
      DriverContext driverContext) {
    this.source = source;
    this.bytes = bytes;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock bytesBlock = (LongBlock) bytes.eval(page)) {
      LongVector bytesVector = bytesBlock.asVector();
      if (bytesVector == null) {
        return eval(page.getPositionCount(), bytesBlock);
      }
      return eval(page.getPositionCount(), bytesVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += bytes.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, LongBlock bytesBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
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
        long bytes = bytesBlock.getLong(bytesBlock.getFirstValueIndex(p));
        try {
          result.appendBytesRef(FmtBytesSi.processLong(bytes));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, LongVector bytesVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long bytes = bytesVector.getLong(p);
        try {
          result.appendBytesRef(FmtBytesSi.processLong(bytes));
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
    return "FmtBytesSiFromLongEvaluator[" + "bytes=" + bytes + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(bytes);
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

    public Factory(Source source, ExpressionEvaluator.Factory bytes) {
      this.source = source;
      this.bytes = bytes;
    }

    @Override
    public FmtBytesSiFromLongEvaluator get(DriverContext context) {
      return new FmtBytesSiFromLongEvaluator(source, bytes.get(context), context);
    }

    @Override
    public String toString() {
      return "FmtBytesSiFromLongEvaluator[" + "bytes=" + bytes + "]";
    }
  }
}
