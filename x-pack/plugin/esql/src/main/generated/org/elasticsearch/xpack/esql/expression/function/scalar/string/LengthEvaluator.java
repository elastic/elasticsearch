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
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Length}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class LengthEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LengthEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator val;

  private final DriverContext driverContext;

  private Warnings warnings;

  public LengthEvaluator(Source source, EvalOperator.ExpressionEvaluator val,
      DriverContext driverContext) {
    this.source = source;
    this.val = val;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock valBlock = (BytesRefBlock) val.eval(page)) {
      var valOrdinalBlock = valBlock.asOrdinals();
      if (valOrdinalBlock != null) {
        return evalOrdinals(page.getPositionCount(), valOrdinalBlock);
      }
      BytesRefVector valVector = valBlock.asVector();
      if (valVector == null) {
        return eval(page.getPositionCount(), valBlock);
      }
      return eval(page.getPositionCount(), valVector).asBlock();
    }
  }

  public IntBlock evalOrdinals(int positionCount, OrdinalBytesRefBlock valBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      var valVector = valBlock.getDictionaryVector();
      var ordinalPositions = valBlock.getOrdinalsBlock();
      try(var dictResult = eval(ordinalPositions.getPositionCount(), valVector)) {
        for (int p = 0; p < positionCount; p++) {
          if (ordinalPositions.isNull(p)) {
            result.appendNull();
            continue;
          }
          var firstValueIndex = ordinalPositions.getFirstValueIndex(p);
          var valueCount = ordinalPositions.getValueCount(p);
          if (valueCount == 1) {
            result.appendInt(dictResult.getInt(ordinalPositions.getInt(firstValueIndex)));
          } else {
            int lastValueIndex = firstValueIndex + valueCount;
            result.beginPositionEntry();
            for (int v = firstValueIndex; v < lastValueIndex; v++) {
              result.appendInt(dictResult.getInt(ordinalPositions.getInt(v)));
            }
            result.endPositionEntry();
          }
        }
      }
      return result.build();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += val.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, BytesRefBlock valBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      BytesRef valScratch = new BytesRef();
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
        BytesRef val = valBlock.getBytesRef(valBlock.getFirstValueIndex(p), valScratch);
        result.appendInt(Length.process(val));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, BytesRefVector valVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      BytesRef valScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef val = valVector.getBytesRef(p, valScratch);
        result.appendInt(p, Length.process(val));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "LengthEvaluator[" + "val=" + val + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val);
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

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory val) {
      this.source = source;
      this.val = val;
    }

    @Override
    public LengthEvaluator get(DriverContext context) {
      return new LengthEvaluator(source, val.get(context), context);
    }

    @Override
    public String toString() {
      return "LengthEvaluator[" + "val=" + val + "]";
    }
  }
}
