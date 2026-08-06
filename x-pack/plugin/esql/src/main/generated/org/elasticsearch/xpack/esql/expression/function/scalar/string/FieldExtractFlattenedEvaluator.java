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
 * {@link ExpressionEvaluator} implementation for {@link FieldExtractFlattened}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class FieldExtractFlattenedEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FieldExtractFlattenedEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator flattenedJson;

  private final ExpressionEvaluator path;

  private final ExpressionEvaluator injectedKey;

  private final DriverContext driverContext;

  private Warnings warnings;

  public FieldExtractFlattenedEvaluator(Source source, ExpressionEvaluator flattenedJson,
      ExpressionEvaluator path, ExpressionEvaluator injectedKey, DriverContext driverContext) {
    this.source = source;
    this.flattenedJson = flattenedJson;
    this.path = path;
    this.injectedKey = injectedKey;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock flattenedJsonBlock = (BytesRefBlock) flattenedJson.eval(page)) {
      try (BytesRefBlock pathBlock = (BytesRefBlock) path.eval(page)) {
        try (BytesRefBlock injectedKeyBlock = (BytesRefBlock) injectedKey.eval(page)) {
          BytesRefVector flattenedJsonVector = flattenedJsonBlock.asVector();
          if (flattenedJsonVector == null) {
            return eval(page.getPositionCount(), flattenedJsonBlock, pathBlock, injectedKeyBlock);
          }
          BytesRefVector pathVector = pathBlock.asVector();
          if (pathVector == null) {
            return eval(page.getPositionCount(), flattenedJsonBlock, pathBlock, injectedKeyBlock);
          }
          BytesRefVector injectedKeyVector = injectedKeyBlock.asVector();
          if (injectedKeyVector == null) {
            return eval(page.getPositionCount(), flattenedJsonBlock, pathBlock, injectedKeyBlock);
          }
          return eval(page.getPositionCount(), flattenedJsonVector, pathVector, injectedKeyVector);
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += flattenedJson.baseRamBytesUsed();
    baseRamBytesUsed += path.baseRamBytesUsed();
    baseRamBytesUsed += injectedKey.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock flattenedJsonBlock,
      BytesRefBlock pathBlock, BytesRefBlock injectedKeyBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef flattenedJsonScratch = new BytesRef();
      BytesRef pathScratch = new BytesRef();
      BytesRef injectedKeyScratch = new BytesRef();
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
        switch (injectedKeyBlock.getValueCount(p)) {
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
        BytesRef injectedKey = injectedKeyBlock.getBytesRef(injectedKeyBlock.getFirstValueIndex(p), injectedKeyScratch);
        try {
          FieldExtractFlattened.process(result, flattenedJson, path, injectedKey);
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector flattenedJsonVector,
      BytesRefVector pathVector, BytesRefVector injectedKeyVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef flattenedJsonScratch = new BytesRef();
      BytesRef pathScratch = new BytesRef();
      BytesRef injectedKeyScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef flattenedJson = flattenedJsonVector.getBytesRef(p, flattenedJsonScratch);
        BytesRef path = pathVector.getBytesRef(p, pathScratch);
        BytesRef injectedKey = injectedKeyVector.getBytesRef(p, injectedKeyScratch);
        try {
          FieldExtractFlattened.process(result, flattenedJson, path, injectedKey);
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
    return "FieldExtractFlattenedEvaluator[" + "flattenedJson=" + flattenedJson + ", path=" + path + ", injectedKey=" + injectedKey + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(flattenedJson, path, injectedKey);
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

    private final ExpressionEvaluator.Factory injectedKey;

    public Factory(Source source, ExpressionEvaluator.Factory flattenedJson,
        ExpressionEvaluator.Factory path, ExpressionEvaluator.Factory injectedKey) {
      this.source = source;
      this.flattenedJson = flattenedJson;
      this.path = path;
      this.injectedKey = injectedKey;
    }

    @Override
    public FieldExtractFlattenedEvaluator get(DriverContext context) {
      return new FieldExtractFlattenedEvaluator(source, flattenedJson.get(context), path.get(context), injectedKey.get(context), context);
    }

    @Override
    public String toString() {
      return "FieldExtractFlattenedEvaluator[" + "flattenedJson=" + flattenedJson + ", path=" + path + ", injectedKey=" + injectedKey + "]";
    }
  }
}
