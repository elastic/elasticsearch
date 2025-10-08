// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.time.ZoneId;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateExtract}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DateExtractMillisEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateExtractMillisEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator value;

  private final EvalOperator.ExpressionEvaluator chronoField;

  private final ZoneId zone;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateExtractMillisEvaluator(Source source, EvalOperator.ExpressionEvaluator value,
      EvalOperator.ExpressionEvaluator chronoField, ZoneId zone, DriverContext driverContext) {
    this.source = source;
    this.value = value;
    this.chronoField = chronoField;
    this.zone = zone;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock valueBlock = (LongBlock) value.eval(page)) {
      try (BytesRefBlock chronoFieldBlock = (BytesRefBlock) chronoField.eval(page)) {
        LongVector valueVector = valueBlock.asVector();
        if (valueVector == null) {
          return eval(page.getPositionCount(), valueBlock, chronoFieldBlock);
        }
        BytesRefVector chronoFieldVector = chronoFieldBlock.asVector();
        if (chronoFieldVector == null) {
          return eval(page.getPositionCount(), valueBlock, chronoFieldBlock);
        }
        return eval(page.getPositionCount(), valueVector, chronoFieldVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += value.baseRamBytesUsed();
    baseRamBytesUsed += chronoField.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, LongBlock valueBlock, BytesRefBlock chronoFieldBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      BytesRef chronoFieldScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (valueBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        switch (chronoFieldBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        long value = valueBlock.getLong(valueBlock.getFirstValueIndex(p));
        BytesRef chronoField = chronoFieldBlock.getBytesRef(chronoFieldBlock.getFirstValueIndex(p), chronoFieldScratch);
        try {
          result.appendLong(DateExtract.processMillis(value, chronoField, this.zone));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public LongBlock eval(int positionCount, LongVector valueVector,
      BytesRefVector chronoFieldVector) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      BytesRef chronoFieldScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        long value = valueVector.getLong(p);
        BytesRef chronoField = chronoFieldVector.getBytesRef(p, chronoFieldScratch);
        try {
          result.appendLong(DateExtract.processMillis(value, chronoField, this.zone));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateExtractMillisEvaluator[" + "value=" + value + ", chronoField=" + chronoField + ", zone=" + zone + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(value, chronoField);
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

    private final EvalOperator.ExpressionEvaluator.Factory chronoField;

    private final ZoneId zone;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory value,
        EvalOperator.ExpressionEvaluator.Factory chronoField, ZoneId zone) {
      this.source = source;
      this.value = value;
      this.chronoField = chronoField;
      this.zone = zone;
    }

    @Override
    public DateExtractMillisEvaluator get(DriverContext context) {
      return new DateExtractMillisEvaluator(source, value.get(context), chronoField.get(context), zone, context);
    }

    @Override
    public String toString() {
      return "DateExtractMillisEvaluator[" + "value=" + value + ", chronoField=" + chronoField + ", zone=" + zone + "]";
    }
  }
}
