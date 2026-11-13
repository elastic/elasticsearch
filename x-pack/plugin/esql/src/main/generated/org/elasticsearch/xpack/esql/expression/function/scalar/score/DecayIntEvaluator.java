// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.score;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Decay}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DecayIntEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DecayIntEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator value;

  private final DecayScorers.NumericScorer scorer;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DecayIntEvaluator(Source source, ExpressionEvaluator value,
      DecayScorers.NumericScorer scorer, DriverContext driverContext) {
    this.source = source;
    this.value = value;
    this.scorer = scorer;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock valueBlock = (IntBlock) value.eval(page)) {
      IntVector valueVector = valueBlock.asVector();
      if (valueVector == null) {
        return eval(page.getPositionCount(), valueBlock);
      }
      return eval(page.getPositionCount(), valueVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += value.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, IntBlock valueBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (valueBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (valueBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        int value = valueBlock.getInt(valueBlock.getFirstValueIndex(p));
        result.appendDouble(Decay.process(value, this.scorer));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, IntVector valueVector) {
    try(DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        int value = valueVector.getInt(p);
        result.appendDouble(p, Decay.process(value, this.scorer));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DecayIntEvaluator[" + "value=" + value + ", scorer=" + scorer + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(value);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory value;

    private final DecayScorers.NumericScorer scorer;

    public Factory(Source source, ExpressionEvaluator.Factory value,
        DecayScorers.NumericScorer scorer) {
      this.source = source;
      this.value = value;
      this.scorer = scorer;
    }

    @Override
    public DecayIntEvaluator get(DriverContext context) {
      return new DecayIntEvaluator(source, value.get(context), scorer, context);
    }

    @Override
    public String toString() {
      return "DecayIntEvaluator[" + "value=" + value + ", scorer=" + scorer + "]";
    }
  }
}
