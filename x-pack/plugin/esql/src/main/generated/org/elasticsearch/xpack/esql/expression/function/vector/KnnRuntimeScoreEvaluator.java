// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.vector;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
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
public final class KnnRuntimeScoreEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(KnnRuntimeScoreEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator fieldBlock;

  private final float[] queryVector;

  private final DenseVectorFieldMapper.SimilarityFunction similarityFunction;

  private final float boost;

  private final DriverContext driverContext;

  private Warnings warnings;

  public KnnRuntimeScoreEvaluator(Source source, ExpressionEvaluator fieldBlock,
      float[] queryVector, DenseVectorFieldMapper.SimilarityFunction similarityFunction,
      float boost, DriverContext driverContext) {
    this.source = source;
    this.fieldBlock = fieldBlock;
    this.queryVector = queryVector;
    this.similarityFunction = similarityFunction;
    this.boost = boost;
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

  public DoubleBlock eval(int positionCount, FloatBlock fieldBlockBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendDouble(Knn.runtimeScore(p, fieldBlockBlock, this.queryVector, this.similarityFunction, this.boost));
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
    return "KnnRuntimeScoreEvaluator[" + "fieldBlock=" + fieldBlock + ", queryVector=" + queryVector + ", similarityFunction=" + similarityFunction + ", boost=" + boost + "]";
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

    private final float boost;

    public Factory(Source source, ExpressionEvaluator.Factory fieldBlock, float[] queryVector,
        DenseVectorFieldMapper.SimilarityFunction similarityFunction, float boost) {
      this.source = source;
      this.fieldBlock = fieldBlock;
      this.queryVector = queryVector;
      this.similarityFunction = similarityFunction;
      this.boost = boost;
    }

    @Override
    public KnnRuntimeScoreEvaluator get(DriverContext context) {
      return new KnnRuntimeScoreEvaluator(source, fieldBlock.get(context), queryVector, similarityFunction, boost, context);
    }

    @Override
    public String toString() {
      return "KnnRuntimeScoreEvaluator[" + "fieldBlock=" + fieldBlock + ", queryVector=" + queryVector + ", similarityFunction=" + similarityFunction + ", boost=" + boost + "]";
    }
  }
}
