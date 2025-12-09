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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Locate}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class LocateEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LocateEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator substr;

  private final EvalOperator.ExpressionEvaluator start;

  private final DriverContext driverContext;

  private Warnings warnings;

  public LocateEvaluator(Source source, EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator substr, EvalOperator.ExpressionEvaluator start,
      DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.substr = substr;
    this.start = start;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (BytesRefBlock substrBlock = (BytesRefBlock) substr.eval(page)) {
        try (IntBlock startBlock = (IntBlock) start.eval(page)) {
          BytesRefVector strVector = strBlock.asVector();
          if (strVector == null) {
            return eval(page.getPositionCount(), strBlock, substrBlock, startBlock);
          }
          BytesRefVector substrVector = substrBlock.asVector();
          if (substrVector == null) {
            return eval(page.getPositionCount(), strBlock, substrBlock, startBlock);
          }
          IntVector startVector = startBlock.asVector();
          if (startVector == null) {
            return eval(page.getPositionCount(), strBlock, substrBlock, startBlock);
          }
          return eval(page.getPositionCount(), strVector, substrVector, startVector).asBlock();
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += str.baseRamBytesUsed();
    baseRamBytesUsed += substr.baseRamBytesUsed();
    baseRamBytesUsed += start.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock substrBlock,
      IntBlock startBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef substrScratch = new BytesRef();
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
        switch (substrBlock.getValueCount(p)) {
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
        BytesRef str = strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch);
        BytesRef substr = substrBlock.getBytesRef(substrBlock.getFirstValueIndex(p), substrScratch);
        int start = startBlock.getInt(startBlock.getFirstValueIndex(p));
        result.appendInt(Locate.process(str, substr, start));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, BytesRefVector strVector, BytesRefVector substrVector,
      IntVector startVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef substrScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef str = strVector.getBytesRef(p, strScratch);
        BytesRef substr = substrVector.getBytesRef(p, substrScratch);
        int start = startVector.getInt(p);
        result.appendInt(p, Locate.process(str, substr, start));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "LocateEvaluator[" + "str=" + str + ", substr=" + substr + ", start=" + start + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, substr, start);
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

    private final EvalOperator.ExpressionEvaluator.Factory substr;

    private final EvalOperator.ExpressionEvaluator.Factory start;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory substr,
        EvalOperator.ExpressionEvaluator.Factory start) {
      this.source = source;
      this.str = str;
      this.substr = substr;
      this.start = start;
    }

    @Override
    public LocateEvaluator get(DriverContext context) {
      return new LocateEvaluator(source, str.get(context), substr.get(context), start.get(context), context);
    }

    @Override
    public String toString() {
      return "LocateEvaluator[" + "str=" + str + ", substr=" + substr + ", start=" + start + "]";
    }
  }
}
