// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.vector;

import java.lang.Float;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Knn}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class KnnRuntimeFilterEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(KnnRuntimeFilterEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator fieldBlock;

  private final float[] queryVector;

  private final DenseVectorFieldMapper.SimilarityFunction similarityFunction;

  private final Float similarityThreshold;

  private final DriverContext driverContext;

  private Warnings warnings;

  public KnnRuntimeFilterEvaluator(Source source, ExpressionEvaluator fieldBlock,
      float[] queryVector, DenseVectorFieldMapper.SimilarityFunction similarityFunction,
      Float similarityThreshold, DriverContext driverContext) {
    this.source = source;
    this.fieldBlock = fieldBlock;
    this.queryVector = queryVector;
    this.similarityFunction = similarityFunction;
    this.similarityThreshold = similarityThreshold;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (FloatBlock fieldBlockBlock = (FloatBlock) fieldBlock.eval(page)) {
      return eval(page.getPositionCount(), fieldBlockBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += fieldBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, FloatBlock fieldBlockBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBoolean(Knn.runtimeFilter(p, fieldBlockBlock, this.queryVector, this.similarityFunction, this.similarityThreshold));
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
    return "KnnRuntimeFilterEvaluator[" + "fieldBlock=" + fieldBlock + ", queryVector=" + queryVector + ", similarityFunction=" + similarityFunction + ", similarityThreshold=" + similarityThreshold + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(fieldBlock);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory fieldBlock;

    private final float[] queryVector;

    private final DenseVectorFieldMapper.SimilarityFunction similarityFunction;

    private final Float similarityThreshold;

    public Factory(Source source, ExpressionEvaluator.Factory fieldBlock, float[] queryVector,
        DenseVectorFieldMapper.SimilarityFunction similarityFunction, Float similarityThreshold) {
      this.source = source;
      this.fieldBlock = fieldBlock;
      this.queryVector = queryVector;
      this.similarityFunction = similarityFunction;
      this.similarityThreshold = similarityThreshold;
    }

    @Override
    public KnnRuntimeFilterEvaluator get(DriverContext context) {
      return new KnnRuntimeFilterEvaluator(source, fieldBlock.get(context), queryVector, similarityFunction, similarityThreshold, context);
    }

    @Override
    public String toString() {
      return "KnnRuntimeFilterEvaluator[" + "fieldBlock=" + fieldBlock + ", queryVector=" + queryVector + ", similarityFunction=" + similarityFunction + ", similarityThreshold=" + similarityThreshold + "]";
    }
  }
}
