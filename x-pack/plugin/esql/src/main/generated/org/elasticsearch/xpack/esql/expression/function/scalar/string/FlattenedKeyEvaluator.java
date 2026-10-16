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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link FlattenedKey}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class FlattenedKeyEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FlattenedKeyEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator flattened;

  private final String key;

  private final DriverContext driverContext;

  private Warnings warnings;

  public FlattenedKeyEvaluator(Source source, ExpressionEvaluator flattened, String key,
      DriverContext driverContext) {
    this.source = source;
    this.flattened = flattened;
    this.key = key;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock flattenedBlock = (BytesRefBlock) flattened.eval(page)) {
      BytesRefVector flattenedVector = flattenedBlock.asVector();
      if (flattenedVector == null) {
        return eval(page.getPositionCount(), flattenedBlock);
      }
      return eval(page.getPositionCount(), flattenedVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += flattened.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock flattenedBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef flattenedScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (flattenedBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (flattenedBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        BytesRef flattened = flattenedBlock.getBytesRef(flattenedBlock.getFirstValueIndex(p), flattenedScratch);
        try {
          FlattenedKey.process(result, flattened, this.key);
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector flattenedVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef flattenedScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef flattened = flattenedVector.getBytesRef(p, flattenedScratch);
        try {
          FlattenedKey.process(result, flattened, this.key);
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
    return "FlattenedKeyEvaluator[" + "flattened=" + flattened + ", key=" + key + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(flattened);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory flattened;

    private final String key;

    public Factory(Source source, ExpressionEvaluator.Factory flattened, String key) {
      this.source = source;
      this.flattened = flattened;
      this.key = key;
    }

    @Override
    public FlattenedKeyEvaluator get(DriverContext context) {
      return new FlattenedKeyEvaluator(source, flattened.get(context), key, context);
    }

    @Override
    public String toString() {
      return "FlattenedKeyEvaluator[" + "flattened=" + flattened + ", key=" + key + "]";
    }
  }
}
