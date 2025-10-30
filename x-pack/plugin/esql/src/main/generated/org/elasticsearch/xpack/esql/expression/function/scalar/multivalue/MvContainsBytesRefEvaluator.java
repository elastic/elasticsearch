// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvContains}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class MvContainsBytesRefEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvContainsBytesRefEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator superset;

  private final EvalOperator.ExpressionEvaluator subset;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvContainsBytesRefEvaluator(Source source, EvalOperator.ExpressionEvaluator superset,
      EvalOperator.ExpressionEvaluator subset, DriverContext driverContext) {
    this.source = source;
    this.superset = superset;
    this.subset = subset;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock supersetBlock = (BytesRefBlock) superset.eval(page)) {
      try (BytesRefBlock subsetBlock = (BytesRefBlock) subset.eval(page)) {
        return eval(page.getPositionCount(), supersetBlock, subsetBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += superset.baseRamBytesUsed();
    baseRamBytesUsed += subset.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock supersetBlock,
      BytesRefBlock subsetBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(MvContains.process(p, supersetBlock, subsetBlock));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvContainsBytesRefEvaluator[" + "superset=" + superset + ", subset=" + subset + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(superset, subset);
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

    private final EvalOperator.ExpressionEvaluator.Factory superset;

    private final EvalOperator.ExpressionEvaluator.Factory subset;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory superset,
        EvalOperator.ExpressionEvaluator.Factory subset) {
      this.source = source;
      this.superset = superset;
      this.subset = subset;
    }

    @Override
    public MvContainsBytesRefEvaluator get(DriverContext context) {
      return new MvContainsBytesRefEvaluator(source, superset.get(context), subset.get(context), context);
    }

    @Override
    public String toString() {
      return "MvContainsBytesRefEvaluator[" + "superset=" + superset + ", subset=" + subset + "]";
    }
  }
}
