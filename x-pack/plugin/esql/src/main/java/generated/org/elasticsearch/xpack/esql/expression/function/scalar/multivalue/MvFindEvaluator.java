// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import java.util.regex.Pattern;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvFind}.
 * This class is generated. Do not edit it.
 */
public final class MvFindEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator field;

  private final Pattern pattern;

  private final DriverContext driverContext;

  public MvFindEvaluator(Source source, EvalOperator.ExpressionEvaluator field, Pattern pattern,
      DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.field = field;
    this.pattern = pattern;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock fieldBlock = (BytesRefBlock) field.eval(page)) {
      return eval(page.getPositionCount(), fieldBlock);
    }
  }

  public IntBlock eval(int positionCount, BytesRefBlock fieldBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!fieldBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        MvFind.process(result, p, fieldBlock, pattern);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvFindEvaluator[" + "field=" + field + ", pattern=" + pattern + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory field;

    private final Pattern pattern;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field, Pattern pattern) {
      this.source = source;
      this.field = field;
      this.pattern = pattern;
    }

    @Override
    public MvFindEvaluator get(DriverContext context) {
      return new MvFindEvaluator(source, field.get(context), pattern, context);
    }

    @Override
    public String toString() {
      return "MvFindEvaluator[" + "field=" + field + ", pattern=" + pattern + "]";
    }
  }
}
