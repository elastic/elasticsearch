// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Chicken}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ChickenEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ChickenEvaluator.class);

  private final Source source;

  private final BreakingBytesRefBuilder scratch;

  private final EvalOperator.ExpressionEvaluator message;

  private final EvalOperator.ExpressionEvaluator style;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ChickenEvaluator(Source source, BreakingBytesRefBuilder scratch,
      EvalOperator.ExpressionEvaluator message, EvalOperator.ExpressionEvaluator style,
      DriverContext driverContext) {
    this.source = source;
    this.scratch = scratch;
    this.message = message;
    this.style = style;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock messageBlock = (BytesRefBlock) message.eval(page)) {
      try (BytesRefBlock styleBlock = (BytesRefBlock) style.eval(page)) {
        BytesRefVector messageVector = messageBlock.asVector();
        if (messageVector == null) {
          return eval(page.getPositionCount(), messageBlock, styleBlock);
        }
        BytesRefVector styleVector = styleBlock.asVector();
        if (styleVector == null) {
          return eval(page.getPositionCount(), messageBlock, styleBlock);
        }
        return eval(page.getPositionCount(), messageVector, styleVector).asBlock();
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += message.baseRamBytesUsed();
    baseRamBytesUsed += style.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock messageBlock,
      BytesRefBlock styleBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef messageScratch = new BytesRef();
      BytesRef styleScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (messageBlock.getValueCount(p)) {
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
        switch (styleBlock.getValueCount(p)) {
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
        BytesRef message = messageBlock.getBytesRef(messageBlock.getFirstValueIndex(p), messageScratch);
        BytesRef style = styleBlock.getBytesRef(styleBlock.getFirstValueIndex(p), styleScratch);
        result.appendBytesRef(Chicken.process(this.scratch, message, style));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector messageVector,
      BytesRefVector styleVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef messageScratch = new BytesRef();
      BytesRef styleScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef message = messageVector.getBytesRef(p, messageScratch);
        BytesRef style = styleVector.getBytesRef(p, styleScratch);
        result.appendBytesRef(Chicken.process(this.scratch, message, style));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ChickenEvaluator[" + "message=" + message + ", style=" + style + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(scratch, message, style);
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

    private final Function<DriverContext, BreakingBytesRefBuilder> scratch;

    private final EvalOperator.ExpressionEvaluator.Factory message;

    private final EvalOperator.ExpressionEvaluator.Factory style;

    public Factory(Source source, Function<DriverContext, BreakingBytesRefBuilder> scratch,
        EvalOperator.ExpressionEvaluator.Factory message,
        EvalOperator.ExpressionEvaluator.Factory style) {
      this.source = source;
      this.scratch = scratch;
      this.message = message;
      this.style = style;
    }

    @Override
    public ChickenEvaluator get(DriverContext context) {
      return new ChickenEvaluator(source, scratch.apply(context), message.get(context), style.get(context), context);
    }

    @Override
    public String toString() {
      return "ChickenEvaluator[" + "message=" + message + ", style=" + style + "]";
    }
  }
}
