// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateFormat}.
 * This class is generated. Do not edit it.
 */
public final class DateFormatConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator val;

  private final DateFormatter formatter;

  private final DriverContext driverContext;

  public DateFormatConstantEvaluator(Source source, EvalOperator.ExpressionEvaluator val,
      DateFormatter formatter, DriverContext driverContext) {
    this.val = val;
    this.formatter = formatter;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock valBlock = (LongBlock) val.eval(page)) {
      LongVector valVector = valBlock.asVector();
      if (valVector == null) {
        return eval(page.getPositionCount(), valBlock);
      }
      return eval(page.getPositionCount(), valVector).asBlock();
    }
  }

  public BytesRefBlock eval(int positionCount, LongBlock valBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (valBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (valBlock.getValueCount(p) != 1) {
          if (valBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBytesRef(DateFormat.process(valBlock.getLong(valBlock.getFirstValueIndex(p)), this.formatter));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, LongVector valVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(DateFormat.process(valVector.getLong(p), this.formatter));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateFormatConstantEvaluator[" + "val=" + val + ", formatter=" + formatter + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory val;

    private final DateFormatter formatter;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory val,
        DateFormatter formatter) {
      this.source = source;
      this.val = val;
      this.formatter = formatter;
    }

    @Override
    public DateFormatConstantEvaluator get(DriverContext context) {
      return new DateFormatConstantEvaluator(source, val.get(context), formatter, context);
    }

    @Override
    public String toString() {
      return "DateFormatConstantEvaluator[" + "val=" + val + ", formatter=" + formatter + "]";
    }
  }
}
