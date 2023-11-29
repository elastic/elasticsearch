// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Override;
import java.lang.String;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateExtract}.
 * This class is generated. Do not edit it.
 */
public final class DateExtractConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator value;

  private final ChronoField chronoField;

  private final ZoneId zone;

  private final DriverContext driverContext;

  public DateExtractConstantEvaluator(EvalOperator.ExpressionEvaluator value,
      ChronoField chronoField, ZoneId zone, DriverContext driverContext) {
    this.value = value;
    this.chronoField = chronoField;
    this.zone = zone;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock valueBlock = (LongBlock) value.eval(page)) {
      LongVector valueVector = valueBlock.asVector();
      if (valueVector == null) {
        return eval(page.getPositionCount(), valueBlock);
      }
      return eval(page.getPositionCount(), valueVector).asBlock();
    }
  }

  public LongBlock eval(int positionCount, LongBlock valueBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (valueBlock.isNull(p) || valueBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        result.appendLong(DateExtract.process(valueBlock.getLong(valueBlock.getFirstValueIndex(p)), chronoField, zone));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector valueVector) {
    try(LongVector.Builder result = driverContext.blockFactory().newLongVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendLong(DateExtract.process(valueVector.getLong(p), chronoField, zone));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateExtractConstantEvaluator[" + "value=" + value + ", chronoField=" + chronoField + ", zone=" + zone + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(value);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory value;

    private final ChronoField chronoField;

    private final ZoneId zone;

    public Factory(EvalOperator.ExpressionEvaluator.Factory value, ChronoField chronoField,
        ZoneId zone) {
      this.value = value;
      this.chronoField = chronoField;
      this.zone = zone;
    }

    @Override
    public DateExtractConstantEvaluator get(DriverContext context) {
      return new DateExtractConstantEvaluator(value.get(context), chronoField, zone, context);
    }

    @Override
    public String toString() {
      return "DateExtractConstantEvaluator[" + "value=" + value + ", chronoField=" + chronoField + ", zone=" + zone + "]";
    }
  }
}
