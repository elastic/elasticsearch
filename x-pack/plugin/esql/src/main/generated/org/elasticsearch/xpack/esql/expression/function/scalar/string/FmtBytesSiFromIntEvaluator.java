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
 * {@link ExpressionEvaluator} implementation for {@link FmtBytesSi}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class FmtBytesSiFromIntEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FmtBytesSiFromIntEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator bytes;

  private final DriverContext driverContext;

  private Warnings warnings;

  public FmtBytesSiFromIntEvaluator(Source source, ExpressionEvaluator bytes,
      DriverContext driverContext) {
    this.source = source;
    this.bytes = bytes;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock bytesBlock = (IntBlock) bytes.eval(page)) {
      IntVector bytesVector = bytesBlock.asVector();
      if (bytesVector == null) {
        return eval(page.getPositionCount(), bytesBlock);
      }
      return eval(page.getPositionCount(), bytesVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += bytes.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, IntBlock bytesBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
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
        int bytes = bytesBlock.getInt(bytesBlock.getFirstValueIndex(p));
        result.appendBytesRef(FmtBytesSi.processInt(bytes));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, IntVector bytesVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        int bytes = bytesVector.getInt(p);
        result.appendBytesRef(FmtBytesSi.processInt(bytes));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "FmtBytesSiFromIntEvaluator[" + "bytes=" + bytes + "]";
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
    public FmtBytesSiFromIntEvaluator get(DriverContext context) {
      return new FmtBytesSiFromIntEvaluator(source, bytes.get(context), context);
    }

    @Override
    public String toString() {
      return "FmtBytesSiFromIntEvaluator[" + "bytes=" + bytes + "]";
    }
  }
}
