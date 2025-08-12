// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.time.ZoneId;
import java.util.Locale;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DayName}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DayNameNanosEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator val;

  private final ZoneId zoneId;

  private final Locale locale;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DayNameNanosEvaluator(Source source, EvalOperator.ExpressionEvaluator val, ZoneId zoneId,
      Locale locale, DriverContext driverContext) {
    this.source = source;
    this.val = val;
    this.zoneId = zoneId;
    this.locale = locale;
    this.driverContext = driverContext;
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
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBytesRef(DayName.processNanos(valBlock.getLong(valBlock.getFirstValueIndex(p)), this.zoneId, this.locale));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, LongVector valVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(DayName.processNanos(valVector.getLong(p), this.zoneId, this.locale));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DayNameNanosEvaluator[" + "val=" + val + ", zoneId=" + zoneId + ", locale=" + locale + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val);
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

    private final EvalOperator.ExpressionEvaluator.Factory val;

    private final ZoneId zoneId;

    private final Locale locale;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory val, ZoneId zoneId,
        Locale locale) {
      this.source = source;
      this.val = val;
      this.zoneId = zoneId;
      this.locale = locale;
    }

    @Override
    public DayNameNanosEvaluator get(DriverContext context) {
      return new DayNameNanosEvaluator(source, val.get(context), zoneId, locale, context);
    }

    @Override
    public String toString() {
      return "DayNameNanosEvaluator[" + "val=" + val + ", zoneId=" + zoneId + ", locale=" + locale + "]";
    }
  }
}
