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
 * {@link ExpressionEvaluator} implementation for {@link FieldExtract}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class FieldExtractEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FieldExtractEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator flattenedJson;

  private final ExpressionEvaluator path;

  private final DriverContext driverContext;

  private Warnings warnings;

  public FieldExtractEvaluator(Source source, ExpressionEvaluator flattenedJson,
      ExpressionEvaluator path, DriverContext driverContext) {
    this.source = source;
    this.flattenedJson = flattenedJson;
    this.path = path;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock flattenedJsonBlock = (BytesRefBlock) flattenedJson.eval(page)) {
      try (BytesRefBlock pathBlock = (BytesRefBlock) path.eval(page)) {
        BytesRefVector flattenedJsonVector = flattenedJsonBlock.asVector();
        if (flattenedJsonVector == null) {
          return eval(page.getPositionCount(), flattenedJsonBlock, pathBlock);
        }
        BytesRefVector pathVector = pathBlock.asVector();
        if (pathVector == null) {
          return eval(page.getPositionCount(), flattenedJsonBlock, pathBlock);
        }
        return eval(page.getPositionCount(), flattenedJsonVector, pathVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += flattenedJson.baseRamBytesUsed();
    baseRamBytesUsed += path.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock flattenedJsonBlock,
      BytesRefBlock pathBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef flattenedJsonScratch = new BytesRef();
      BytesRef pathScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (flattenedJsonBlock.getValueCount(p)) {
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
        switch (pathBlock.getValueCount(p)) {
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
        BytesRef flattenedJson = flattenedJsonBlock.getBytesRef(flattenedJsonBlock.getFirstValueIndex(p), flattenedJsonScratch);
        BytesRef path = pathBlock.getBytesRef(pathBlock.getFirstValueIndex(p), pathScratch);
        try {
          FieldExtract.process(result, flattenedJson, path);
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector flattenedJsonVector,
      BytesRefVector pathVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef flattenedJsonScratch = new BytesRef();
      BytesRef pathScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef flattenedJson = flattenedJsonVector.getBytesRef(p, flattenedJsonScratch);
        BytesRef path = pathVector.getBytesRef(p, pathScratch);
        try {
          FieldExtract.process(result, flattenedJson, path);
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
    return "FieldExtractEvaluator[" + "flattenedJson=" + flattenedJson + ", path=" + path + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(flattenedJson, path);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory flattenedJson;

    private final ExpressionEvaluator.Factory path;

    public Factory(Source source, ExpressionEvaluator.Factory flattenedJson,
        ExpressionEvaluator.Factory path) {
      this.source = source;
      this.flattenedJson = flattenedJson;
      this.path = path;
    }

    @Override
    public FieldExtractEvaluator get(DriverContext context) {
      return new FieldExtractEvaluator(source, flattenedJson.get(context), path.get(context), context);
    }

    @Override
    public String toString() {
      return "FieldExtractEvaluator[" + "flattenedJson=" + flattenedJson + ", path=" + path + "]";
    }
  }
}
