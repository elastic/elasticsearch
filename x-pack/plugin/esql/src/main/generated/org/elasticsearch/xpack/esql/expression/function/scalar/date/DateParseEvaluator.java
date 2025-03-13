// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateParse}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DateParseEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator val;

  private final EvalOperator.ExpressionEvaluator formatter;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateParseEvaluator(Source source, EvalOperator.ExpressionEvaluator val,
      EvalOperator.ExpressionEvaluator formatter, DriverContext driverContext) {
    this.source = source;
    this.val = val;
    this.formatter = formatter;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock valBlock = (BytesRefBlock) val.eval(page)) {
      try (BytesRefBlock formatterBlock = (BytesRefBlock) formatter.eval(page)) {
        BytesRefVector valVector = valBlock.asVector();
        if (valVector == null) {
          return eval(page.getPositionCount(), valBlock, formatterBlock);
        }
        BytesRefVector formatterVector = formatterBlock.asVector();
        if (formatterVector == null) {
          return eval(page.getPositionCount(), valBlock, formatterBlock);
        }
        return eval(page.getPositionCount(), valVector, formatterVector);
      }
    }
  }

  public LongBlock eval(int positionCount, BytesRefBlock valBlock, BytesRefBlock formatterBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      BytesRef valScratch = new BytesRef();
      BytesRef formatterScratch = new BytesRef();
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
        if (formatterBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (formatterBlock.getValueCount(p) != 1) {
          if (formatterBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendLong(DateParse.process(valBlock.getBytesRef(valBlock.getFirstValueIndex(p), valScratch), formatterBlock.getBytesRef(formatterBlock.getFirstValueIndex(p), formatterScratch)));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public LongBlock eval(int positionCount, BytesRefVector valVector,
      BytesRefVector formatterVector) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      BytesRef valScratch = new BytesRef();
      BytesRef formatterScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendLong(DateParse.process(valVector.getBytesRef(p, valScratch), formatterVector.getBytesRef(p, formatterScratch)));
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
    return "DateParseEvaluator[" + "val=" + val + ", formatter=" + formatter + "]";
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

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory val,
        EvalOperator.ExpressionEvaluator.Factory formatter) {
      this.source = source;
      this.val = val;
      this.formatter = formatter;
    }

    @Override
    public DateParseEvaluator get(DriverContext context) {
      return new DateParseEvaluator(source, val.get(context), formatter.get(context), context);
    }

    @Override
    public String toString() {
      return "DateParseEvaluator[" + "val=" + val + ", formatter=" + formatter + "]";
    }
  }
}
