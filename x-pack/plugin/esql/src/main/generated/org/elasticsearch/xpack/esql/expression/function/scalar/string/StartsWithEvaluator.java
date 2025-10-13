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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StartsWith}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StartsWithEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StartsWithEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator prefix;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StartsWithEvaluator(Source source, EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator prefix, DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.prefix = prefix;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (BytesRefBlock prefixBlock = (BytesRefBlock) prefix.eval(page)) {
        BytesRefVector strVector = strBlock.asVector();
        if (strVector == null) {
          return eval(page.getPositionCount(), strBlock, prefixBlock);
        }
        BytesRefVector prefixVector = prefixBlock.asVector();
        if (prefixVector == null) {
          return eval(page.getPositionCount(), strBlock, prefixBlock);
        }
        return eval(page.getPositionCount(), strVector, prefixVector).asBlock();
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += str.baseRamBytesUsed();
    baseRamBytesUsed += prefix.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock prefixBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef prefixScratch = new BytesRef();
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
        switch (prefixBlock.getValueCount(p)) {
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
        BytesRef prefix = prefixBlock.getBytesRef(prefixBlock.getFirstValueIndex(p), prefixScratch);
        result.appendBoolean(StartsWith.process(str, prefix));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BytesRefVector strVector,
      BytesRefVector prefixVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef prefixScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef str = strVector.getBytesRef(p, strScratch);
        BytesRef prefix = prefixVector.getBytesRef(p, prefixScratch);
        result.appendBoolean(p, StartsWith.process(str, prefix));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StartsWithEvaluator[" + "str=" + str + ", prefix=" + prefix + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, prefix);
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

    private final EvalOperator.ExpressionEvaluator.Factory prefix;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory prefix) {
      this.source = source;
      this.str = str;
      this.prefix = prefix;
    }

    @Override
    public StartsWithEvaluator get(DriverContext context) {
      return new StartsWithEvaluator(source, str.get(context), prefix.get(context), context);
    }

    @Override
    public String toString() {
      return "StartsWithEvaluator[" + "str=" + str + ", prefix=" + prefix + "]";
    }
  }
}
