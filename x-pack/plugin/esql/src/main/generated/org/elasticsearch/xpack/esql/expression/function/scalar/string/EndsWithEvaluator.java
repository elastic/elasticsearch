// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link EndsWith}.
 * This class is generated. Do not edit it.
 */
public final class EndsWithEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator suffix;

  private final DriverContext driverContext;

  private Warnings warnings;

  public EndsWithEvaluator(Source source, EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator suffix, DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.suffix = suffix;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (BytesRefBlock suffixBlock = (BytesRefBlock) suffix.eval(page)) {
        BytesRefVector strVector = strBlock.asVector();
        if (strVector == null) {
          return eval(page.getPositionCount(), strBlock, suffixBlock);
        }
        BytesRefVector suffixVector = suffixBlock.asVector();
        if (suffixVector == null) {
          return eval(page.getPositionCount(), strBlock, suffixBlock);
        }
        return eval(page.getPositionCount(), strVector, suffixVector).asBlock();
      }
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock suffixBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef suffixScratch = new BytesRef();
      int accumulatedCost = 0;
      position: for (int p = 0; p < positionCount; p++) {
        if (strBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (strBlock.getValueCount(p) != 1) {
          if (strBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (suffixBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (suffixBlock.getValueCount(p) != 1) {
          if (suffixBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        accumulatedCost += 1;
        if (accumulatedCost >= DriverContext.CHECK_FOR_EARLY_TERMINATION_COST_THRESHOLD) {
          accumulatedCost = 0;
          driverContext.checkForEarlyTermination();
        }
        result.appendBoolean(EndsWith.process(strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), suffixBlock.getBytesRef(suffixBlock.getFirstValueIndex(p), suffixScratch)));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BytesRefVector strVector,
      BytesRefVector suffixVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef suffixScratch = new BytesRef();
      // generate a tight loop to allow vectorization
      int maxBatchSize = Math.max(DriverContext.CHECK_FOR_EARLY_TERMINATION_COST_THRESHOLD / 1, 1);
      for (int start = 0; start < positionCount; ) {
        int end = start + Math.min(positionCount - start, maxBatchSize);
        driverContext.checkForEarlyTermination();
        for (int p = start; p < end; p++) {
          result.appendBoolean(p, EndsWith.process(strVector.getBytesRef(p, strScratch), suffixVector.getBytesRef(p, suffixScratch)));
        }
        start = end;
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "EndsWithEvaluator[" + "str=" + str + ", suffix=" + suffix + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, suffix);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(
              driverContext.warningsMode(),
              source.source().getLineNumber(),
              source.source().getColumnNumber(),
              source.text()
          );
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory str;

    private final EvalOperator.ExpressionEvaluator.Factory suffix;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory suffix) {
      this.source = source;
      this.str = str;
      this.suffix = suffix;
    }

    @Override
    public EndsWithEvaluator get(DriverContext context) {
      return new EndsWithEvaluator(source, str.get(context), suffix.get(context), context);
    }

    @Override
    public String toString() {
      return "EndsWithEvaluator[" + "str=" + str + ", suffix=" + suffix + "]";
    }
  }
}
