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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ReplaceCaptureUntilDelimiter}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ReplaceCaptureUntilDelimiterEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ReplaceCaptureUntilDelimiterEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator str;

  private final ReplaceCaptureUntilDelimiter.Idiom idiom;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ReplaceCaptureUntilDelimiterEvaluator(Source source, ExpressionEvaluator str,
      ReplaceCaptureUntilDelimiter.Idiom idiom, DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.idiom = idiom;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      BytesRefVector strVector = strBlock.asVector();
      if (strVector == null) {
        return eval(page.getPositionCount(), strBlock);
      }
      return eval(page.getPositionCount(), strVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += str.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (strBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (strBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        BytesRef str = strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch);
        try {
          result.appendBytesRef(ReplaceCaptureUntilDelimiter.process(str, this.idiom));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector strVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef str = strVector.getBytesRef(p, strScratch);
        try {
          result.appendBytesRef(ReplaceCaptureUntilDelimiter.process(str, this.idiom));
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
    return "ReplaceCaptureUntilDelimiterEvaluator[" + "str=" + str + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory str;

    private final ReplaceCaptureUntilDelimiter.Idiom idiom;

    public Factory(Source source, ExpressionEvaluator.Factory str,
        ReplaceCaptureUntilDelimiter.Idiom idiom) {
      this.source = source;
      this.str = str;
      this.idiom = idiom;
    }

    @Override
    public ReplaceCaptureUntilDelimiterEvaluator get(DriverContext context) {
      return new ReplaceCaptureUntilDelimiterEvaluator(source, str.get(context), idiom, context);
    }

    @Override
    public String toString() {
      return "ReplaceCaptureUntilDelimiterEvaluator[" + "str=" + str + "]";
    }
  }
}
