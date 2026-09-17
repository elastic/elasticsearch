// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.vector;

import java.lang.Float;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Knn}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class KnnRuntimeFilterUnitVectorEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(KnnRuntimeFilterUnitVectorEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator fieldBlock;

  private final float[] queryVector;

  private final VectorSimilarityMetric similarityMetric;

  private final Float similarityThreshold;

  private final float[] scratchVector;

  private final DriverContext driverContext;

  private Warnings warnings;

  public KnnRuntimeFilterUnitVectorEvaluator(Source source, ExpressionEvaluator fieldBlock,
      float[] queryVector, VectorSimilarityMetric similarityMetric, Float similarityThreshold,
      float[] scratchVector, DriverContext driverContext) {
    this.source = source;
    this.fieldBlock = fieldBlock;
    this.queryVector = queryVector;
    this.similarityMetric = similarityMetric;
    this.similarityThreshold = similarityThreshold;
    this.scratchVector = scratchVector;
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
          result.appendBoolean(Knn.runtimeFilterUnitVector(p, fieldBlockBlock, this.queryVector, this.similarityMetric, this.similarityThreshold, this.scratchVector));
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
    return "KnnRuntimeFilterUnitVectorEvaluator[" + "fieldBlock=" + fieldBlock + ", queryVector=" + queryVector + ", similarityMetric=" + similarityMetric + ", similarityThreshold=" + similarityThreshold + "]";
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

    private final VectorSimilarityMetric similarityMetric;

    private final Float similarityThreshold;

    private final Function<DriverContext, float[]> scratchVector;

    public Factory(Source source, ExpressionEvaluator.Factory fieldBlock, float[] queryVector,
        VectorSimilarityMetric similarityMetric, Float similarityThreshold,
        Function<DriverContext, float[]> scratchVector) {
      this.source = source;
      this.fieldBlock = fieldBlock;
      this.queryVector = queryVector;
      this.similarityMetric = similarityMetric;
      this.similarityThreshold = similarityThreshold;
      this.scratchVector = scratchVector;
    }

    @Override
    public KnnRuntimeFilterUnitVectorEvaluator get(DriverContext context) {
      return new KnnRuntimeFilterUnitVectorEvaluator(source, fieldBlock.get(context), queryVector, similarityMetric, similarityThreshold, scratchVector.apply(context), context);
    }

    @Override
    public String toString() {
      return "KnnRuntimeFilterUnitVectorEvaluator[" + "fieldBlock=" + fieldBlock + ", queryVector=" + queryVector + ", similarityMetric=" + similarityMetric + ", similarityThreshold=" + similarityThreshold + "]";
    }
  }
}
