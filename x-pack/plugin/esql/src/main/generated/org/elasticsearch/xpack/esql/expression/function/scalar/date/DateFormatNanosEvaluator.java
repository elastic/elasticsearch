// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.Locale;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateFormat}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DateFormatNanosEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateFormatNanosEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator val;

  private final EvalOperator.ExpressionEvaluator formatter;

  private final Locale locale;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateFormatNanosEvaluator(Source source, EvalOperator.ExpressionEvaluator val,
      EvalOperator.ExpressionEvaluator formatter, Locale locale, DriverContext driverContext) {
    this.source = source;
    this.val = val;
    this.formatter = formatter;
    this.locale = locale;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock valBlock = (LongBlock) val.eval(page)) {
      try (BytesRefBlock formatterBlock = (BytesRefBlock) formatter.eval(page)) {
        LongVector valVector = valBlock.asVector();
        if (valVector == null) {
          return eval(page.getPositionCount(), valBlock, formatterBlock);
        }
        BytesRefVector formatterVector = formatterBlock.asVector();
        if (formatterVector == null) {
          return eval(page.getPositionCount(), valBlock, formatterBlock);
        }
        return eval(page.getPositionCount(), valVector, formatterVector).asBlock();
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += val.baseRamBytesUsed();
    baseRamBytesUsed += formatter.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, LongBlock valBlock, BytesRefBlock formatterBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef formatterScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (valBlock.getValueCount(p)) {
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
        switch (formatterBlock.getValueCount(p)) {
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
        long val = valBlock.getLong(valBlock.getFirstValueIndex(p));
        BytesRef formatter = formatterBlock.getBytesRef(formatterBlock.getFirstValueIndex(p), formatterScratch);
        result.appendBytesRef(DateFormat.processNanos(val, formatter, this.locale));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, LongVector valVector,
      BytesRefVector formatterVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef formatterScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        long val = valVector.getLong(p);
        BytesRef formatter = formatterVector.getBytesRef(p, formatterScratch);
        result.appendBytesRef(DateFormat.processNanos(val, formatter, this.locale));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateFormatNanosEvaluator[" + "val=" + val + ", formatter=" + formatter + ", locale=" + locale + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val, formatter);
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

    private final EvalOperator.ExpressionEvaluator.Factory formatter;

    private final Locale locale;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory val,
        EvalOperator.ExpressionEvaluator.Factory formatter, Locale locale) {
      this.source = source;
      this.val = val;
      this.formatter = formatter;
      this.locale = locale;
    }

    @Override
    public DateFormatNanosEvaluator get(DriverContext context) {
      return new DateFormatNanosEvaluator(source, val.get(context), formatter.get(context), locale, context);
    }

    @Override
    public String toString() {
      return "DateFormatNanosEvaluator[" + "val=" + val + ", formatter=" + formatter + ", locale=" + locale + "]";
    }
  }
}
