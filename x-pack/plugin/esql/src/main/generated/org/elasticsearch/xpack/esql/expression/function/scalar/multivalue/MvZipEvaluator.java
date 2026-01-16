// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvZip}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class MvZipEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvZipEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator leftField;

  private final EvalOperator.ExpressionEvaluator rightField;

  private final EvalOperator.ExpressionEvaluator delim;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvZipEvaluator(Source source, EvalOperator.ExpressionEvaluator leftField,
      EvalOperator.ExpressionEvaluator rightField, EvalOperator.ExpressionEvaluator delim,
      DriverContext driverContext) {
    this.source = source;
    this.leftField = leftField;
    this.rightField = rightField;
    this.delim = delim;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock leftFieldBlock = (BytesRefBlock) leftField.eval(page)) {
      try (BytesRefBlock rightFieldBlock = (BytesRefBlock) rightField.eval(page)) {
        try (BytesRefBlock delimBlock = (BytesRefBlock) delim.eval(page)) {
          return eval(page.getPositionCount(), leftFieldBlock, rightFieldBlock, delimBlock);
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += leftField.baseRamBytesUsed();
    baseRamBytesUsed += rightField.baseRamBytesUsed();
    baseRamBytesUsed += delim.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock leftFieldBlock,
      BytesRefBlock rightFieldBlock, BytesRefBlock delimBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef delimScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!leftFieldBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!rightFieldBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        switch (delimBlock.getValueCount(p)) {
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
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        BytesRef delim = delimBlock.getBytesRef(delimBlock.getFirstValueIndex(p), delimScratch);
        MvZip.process(result, p, leftFieldBlock, rightFieldBlock, delim);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvZipEvaluator[" + "leftField=" + leftField + ", rightField=" + rightField + ", delim=" + delim + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(leftField, rightField, delim);
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

    private final EvalOperator.ExpressionEvaluator.Factory leftField;

    private final EvalOperator.ExpressionEvaluator.Factory rightField;

    private final EvalOperator.ExpressionEvaluator.Factory delim;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory leftField,
        EvalOperator.ExpressionEvaluator.Factory rightField,
        EvalOperator.ExpressionEvaluator.Factory delim) {
      this.source = source;
      this.leftField = leftField;
      this.rightField = rightField;
      this.delim = delim;
    }

    @Override
    public MvZipEvaluator get(DriverContext context) {
      return new MvZipEvaluator(source, leftField.get(context), rightField.get(context), delim.get(context), context);
    }

    @Override
    public String toString() {
      return "MvZipEvaluator[" + "leftField=" + leftField + ", rightField=" + rightField + ", delim=" + delim + "]";
    }
  }
}
