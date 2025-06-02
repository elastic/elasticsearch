// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ParseIp}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ParseIpLeadingZerosAreDecimalEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator string;

  private final BreakingBytesRefBuilder scratch;

  public ParseIpLeadingZerosAreDecimalEvaluator(Source source,
      EvalOperator.ExpressionEvaluator string, BreakingBytesRefBuilder scratch,
      DriverContext driverContext) {
    super(driverContext, source);
    this.string = string;
    this.scratch = scratch;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return string;
  }

  @Override
  public Block evalVector(Vector v) {
    BytesRefVector vector = (BytesRefVector) v;
    int positionCount = v.getPositionCount();
    BytesRef scratchPad = new BytesRef();
    if (vector.isConstant()) {
      try {
        return driverContext.blockFactory().newConstantBytesRefBlockWith(evalValue(vector, 0, scratchPad), positionCount);
      } catch (IllegalArgumentException  e) {
        registerException(e);
        return driverContext.blockFactory().newConstantNullBlock(positionCount);
      }
    }
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        try {
          builder.appendBytesRef(evalValue(vector, p, scratchPad));
        } catch (IllegalArgumentException  e) {
          registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  private BytesRef evalValue(BytesRefVector container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return ParseIp.leadingZerosAreDecimal(value, this.scratch);
  }

  @Override
  public Block evalBlock(Block b) {
    BytesRefBlock block = (BytesRefBlock) b;
    int positionCount = block.getPositionCount();
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef scratchPad = new BytesRef();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          try {
            BytesRef value = evalValue(block, i, scratchPad);
            if (positionOpened == false && valueCount > 1) {
              builder.beginPositionEntry();
              positionOpened = true;
            }
            builder.appendBytesRef(value);
            valuesAppended = true;
          } catch (IllegalArgumentException  e) {
            registerException(e);
          }
        }
        if (valuesAppended == false) {
          builder.appendNull();
        } else if (positionOpened) {
          builder.endPositionEntry();
        }
      }
      return builder.build();
    }
  }

  private BytesRef evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return ParseIp.leadingZerosAreDecimal(value, this.scratch);
  }

  @Override
  public String toString() {
    return "ParseIpLeadingZerosAreDecimalEvaluator[" + "string=" + string + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(string, scratch);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory string;

    private final Function<DriverContext, BreakingBytesRefBuilder> scratch;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory string,
        Function<DriverContext, BreakingBytesRefBuilder> scratch) {
      this.source = source;
      this.string = string;
      this.scratch = scratch;
    }

    @Override
    public ParseIpLeadingZerosAreDecimalEvaluator get(DriverContext context) {
      return new ParseIpLeadingZerosAreDecimalEvaluator(source, string.get(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "ParseIpLeadingZerosAreDecimalEvaluator[" + "string=" + string + "]";
    }
  }
}
