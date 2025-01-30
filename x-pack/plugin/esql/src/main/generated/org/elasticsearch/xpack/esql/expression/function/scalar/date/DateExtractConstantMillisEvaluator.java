// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
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
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateExtract}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DateExtractConstantMillisEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator value;

  private final ChronoField chronoField;

  private final ZoneId zone;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateExtractConstantMillisEvaluator(Source source, EvalOperator.ExpressionEvaluator value,
      ChronoField chronoField, ZoneId zone, DriverContext driverContext) {
    this.source = source;
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
        if (valueBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (valueBlock.getValueCount(p) != 1) {
          if (valueBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendLong(DateExtract.processMillis(valueBlock.getLong(valueBlock.getFirstValueIndex(p)), this.chronoField, this.zone));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector valueVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendLong(p, DateExtract.processMillis(valueVector.getLong(p), this.chronoField, this.zone));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateExtractConstantMillisEvaluator[" + "value=" + value + ", chronoField=" + chronoField + ", zone=" + zone + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(value);
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

    private final EvalOperator.ExpressionEvaluator.Factory value;

    private final ChronoField chronoField;

    private final ZoneId zone;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory value,
        ChronoField chronoField, ZoneId zone) {
      this.source = source;
      this.value = value;
      this.chronoField = chronoField;
      this.zone = zone;
    }

    @Override
    public DateExtractConstantMillisEvaluator get(DriverContext context) {
      return new DateExtractConstantMillisEvaluator(source, value.get(context), chronoField, zone, context);
    }

    @Override
    public String toString() {
      return "DateExtractConstantMillisEvaluator[" + "value=" + value + ", chronoField=" + chronoField + ", zone=" + zone + "]";
    }
  }
}
