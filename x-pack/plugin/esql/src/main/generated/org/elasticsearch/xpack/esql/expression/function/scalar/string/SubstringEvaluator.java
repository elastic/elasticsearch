// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Substring}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class SubstringEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SubstringEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator start;

  private final EvalOperator.ExpressionEvaluator length;

  private final DriverContext driverContext;

  private Warnings warnings;

  public SubstringEvaluator(Source source, EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator start, EvalOperator.ExpressionEvaluator length,
      DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.start = start;
    this.length = length;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (IntBlock startBlock = (IntBlock) start.eval(page)) {
        try (IntBlock lengthBlock = (IntBlock) length.eval(page)) {
          BytesRefVector strVector = strBlock.asVector();
          if (strVector == null) {
            return eval(page.getPositionCount(), strBlock, startBlock, lengthBlock);
          }
          IntVector startVector = startBlock.asVector();
          if (startVector == null) {
            return eval(page.getPositionCount(), strBlock, startBlock, lengthBlock);
          }
          IntVector lengthVector = lengthBlock.asVector();
          if (lengthVector == null) {
            return eval(page.getPositionCount(), strBlock, startBlock, lengthBlock);
          }
          return eval(page.getPositionCount(), strVector, startVector, lengthVector).asBlock();
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += str.baseRamBytesUsed();
    baseRamBytesUsed += start.baseRamBytesUsed();
    baseRamBytesUsed += length.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, IntBlock startBlock,
      IntBlock lengthBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (strBlock.getValueCount(p)) {
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
        switch (startBlock.getValueCount(p)) {
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
        switch (lengthBlock.getValueCount(p)) {
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
        BytesRef str = strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch);
        int start = startBlock.getInt(startBlock.getFirstValueIndex(p));
        int length = lengthBlock.getInt(lengthBlock.getFirstValueIndex(p));
        result.appendBytesRef(Substring.process(str, start, length));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector strVector, IntVector startVector,
      IntVector lengthVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef str = strVector.getBytesRef(p, strScratch);
        int start = startVector.getInt(p);
        int length = lengthVector.getInt(p);
        result.appendBytesRef(Substring.process(str, start, length));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "SubstringEvaluator[" + "str=" + str + ", start=" + start + ", length=" + length + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, start, length);
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

    private final EvalOperator.ExpressionEvaluator.Factory str;

    private final EvalOperator.ExpressionEvaluator.Factory start;

    private final EvalOperator.ExpressionEvaluator.Factory length;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory start,
        EvalOperator.ExpressionEvaluator.Factory length) {
      this.source = source;
      this.str = str;
      this.start = start;
      this.length = length;
    }

    @Override
    public SubstringEvaluator get(DriverContext context) {
      return new SubstringEvaluator(source, str.get(context), start.get(context), length.get(context), context);
    }

    @Override
    public String toString() {
      return "SubstringEvaluator[" + "str=" + str + ", start=" + start + ", length=" + length + "]";
    }
  }
}
