// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvAppend}.
 * This class is generated. Do not edit it.
 */
public final class MvAppendBytesRefEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator field1;

  private final EvalOperator.ExpressionEvaluator[] field2;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvAppendBytesRefEvaluator(Source source, EvalOperator.ExpressionEvaluator field1,
      EvalOperator.ExpressionEvaluator[] field2, DriverContext driverContext) {
    this.source = source;
    this.field1 = field1;
    this.field2 = field2;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock field1Block = (BytesRefBlock) field1.eval(page)) {
      BytesRefBlock[] field2Blocks = new BytesRefBlock[field2.length];
      try (Releasable field2Release = Releasables.wrap(field2Blocks)) {
        for (int i = 0; i < field2Blocks.length; i++) {
          field2Blocks[i] = (BytesRefBlock)field2[i].eval(page);
        }
        return eval(page.getPositionCount(), field1Block, field2Blocks);
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock field1Block,
      BytesRefBlock[] field2Blocks) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRefBlock[] field2Values = new BytesRefBlock[field2.length];
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!field1Block.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        // unpack field2Blocks into field2Values
        for (int i = 0; i < field2Blocks.length; i++) {
          int o = field2Blocks[i].getFirstValueIndex(p);
          field2Values[i] = field2Blocks[i];
        }
        MvAppend.process(result, p, field1Block, field2Values);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvAppendBytesRefEvaluator[" + "field1=" + field1 + ", field2=" + Arrays.toString(field2) + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field1, () -> Releasables.close(field2));
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

    private final EvalOperator.ExpressionEvaluator.Factory[] field2;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field1,
        EvalOperator.ExpressionEvaluator.Factory[] field2) {
      this.source = source;
      this.field1 = field1;
      this.field2 = field2;
    }

    @Override
    public MvAppendBytesRefEvaluator get(DriverContext context) {
      EvalOperator.ExpressionEvaluator[] field2 = Arrays.stream(this.field2).map(a -> a.get(context)).toArray(EvalOperator.ExpressionEvaluator[]::new);
      return new MvAppendBytesRefEvaluator(source, field1.get(context), field2, context);
    }

    @Override
    public String toString() {
      return "MvAppendBytesRefEvaluator[" + "field1=" + field1 + ", field2=" + Arrays.toString(field2) + "]";
    }
  }
}
