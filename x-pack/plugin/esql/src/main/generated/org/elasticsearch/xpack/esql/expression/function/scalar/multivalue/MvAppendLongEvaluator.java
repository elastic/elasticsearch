// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvAppend}.
 * This class is generated. Do not edit it.
 */
public final class MvAppendLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator field1;

  private final EvalOperator.ExpressionEvaluator field2;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvAppendLongEvaluator(Source source, EvalOperator.ExpressionEvaluator field1,
      EvalOperator.ExpressionEvaluator field2, DriverContext driverContext) {
    this.source = source;
    this.field1 = field1;
    this.field2 = field2;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock field1Block = (LongBlock) field1.eval(page)) {
      try (LongBlock field2Block = (LongBlock) field2.eval(page)) {
        return eval(page.getPositionCount(), field1Block, field2Block);
      }
    }
  }

  public LongBlock eval(int positionCount, LongBlock field1Block, LongBlock field2Block) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!field1Block.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!field2Block.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        MvAppend.process(result, p, field1Block, field2Block);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvAppendLongEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field1, field2);
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

    private final EvalOperator.ExpressionEvaluator.Factory field1;

    private final EvalOperator.ExpressionEvaluator.Factory field2;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field1,
        EvalOperator.ExpressionEvaluator.Factory field2) {
      this.source = source;
      this.field1 = field1;
      this.field2 = field2;
    }

    @Override
    public MvAppendLongEvaluator get(DriverContext context) {
      return new MvAppendLongEvaluator(source, field1.get(context), field2.get(context), context);
    }

    @Override
    public String toString() {
      return "MvAppendLongEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
    }
  }
}
