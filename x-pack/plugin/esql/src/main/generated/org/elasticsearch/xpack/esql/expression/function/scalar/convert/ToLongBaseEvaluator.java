// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToLongBase}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ToLongBaseEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToLongBaseEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator string;

  private final EvalOperator.ExpressionEvaluator base;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ToLongBaseEvaluator(Source source, EvalOperator.ExpressionEvaluator string,
      EvalOperator.ExpressionEvaluator base, DriverContext driverContext) {
    this.source = source;
    this.string = string;
    this.base = base;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock stringBlock = (BytesRefBlock) string.eval(page)) {
      try (IntBlock baseBlock = (IntBlock) base.eval(page)) {
        BytesRefVector stringVector = stringBlock.asVector();
        if (stringVector == null) {
          return eval(page.getPositionCount(), stringBlock, baseBlock);
        }
        IntVector baseVector = baseBlock.asVector();
        if (baseVector == null) {
          return eval(page.getPositionCount(), stringBlock, baseBlock);
        }
        return eval(page.getPositionCount(), stringVector, baseVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += string.baseRamBytesUsed();
    baseRamBytesUsed += base.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, BytesRefBlock stringBlock, IntBlock baseBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      BytesRef stringScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (stringBlock.getValueCount(p)) {
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
        switch (baseBlock.getValueCount(p)) {
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
        BytesRef string = stringBlock.getBytesRef(stringBlock.getFirstValueIndex(p), stringScratch);
        int base = baseBlock.getInt(baseBlock.getFirstValueIndex(p));
        try {
          result.appendLong(ToLongBase.process(string, base));
        } catch (InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public LongBlock eval(int positionCount, BytesRefVector stringVector, IntVector baseVector) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      BytesRef stringScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef string = stringVector.getBytesRef(p, stringScratch);
        int base = baseVector.getInt(p);
        try {
          result.appendLong(ToLongBase.process(string, base));
        } catch (InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ToLongBaseEvaluator[" + "string=" + string + ", base=" + base + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(string, base);
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

    private final EvalOperator.ExpressionEvaluator.Factory string;

    private final EvalOperator.ExpressionEvaluator.Factory base;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory string,
        EvalOperator.ExpressionEvaluator.Factory base) {
      this.source = source;
      this.string = string;
      this.base = base;
    }

    @Override
    public ToLongBaseEvaluator get(DriverContext context) {
      return new ToLongBaseEvaluator(source, string.get(context), base.get(context), context);
    }

    @Override
    public String toString() {
      return "ToLongBaseEvaluator[" + "string=" + string + ", base=" + base + "]";
    }
  }
}
