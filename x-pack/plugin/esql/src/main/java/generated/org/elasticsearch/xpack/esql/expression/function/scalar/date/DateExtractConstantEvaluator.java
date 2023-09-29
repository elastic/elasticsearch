// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Override;
import java.lang.String;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateExtract}.
 * This class is generated. Do not edit it.
 */
public final class DateExtractConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator value;

  private final ChronoField chronoField;

  private final ZoneId zone;

  private final DriverContext driverContext;

  public DateExtractConstantEvaluator(EvalOperator.ExpressionEvaluator value,
      ChronoField chronoField, ZoneId zone, DriverContext driverContext) {
    this.value = value;
    this.chronoField = chronoField;
    this.zone = zone;
    this.driverContext = driverContext;
  }

  @Override
  public Block.Ref eval(Page page) {
    try (Block.Ref valueRef = value.eval(page)) {
      if (valueRef.block().areAllValuesNull()) {
        return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
      }
      LongBlock valueBlock = (LongBlock) valueRef.block();
      LongVector valueVector = valueBlock.asVector();
      if (valueVector == null) {
        return Block.Ref.floating(eval(page.getPositionCount(), valueBlock));
      }
      return Block.Ref.floating(eval(page.getPositionCount(), valueVector).asBlock());
    }
  }

  public LongBlock eval(int positionCount, LongBlock valueBlock) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (valueBlock.isNull(p) || valueBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendLong(DateExtract.process(valueBlock.getLong(valueBlock.getFirstValueIndex(p)), chronoField, zone));
    }
    return result.build();
  }

  public LongVector eval(int positionCount, LongVector valueVector) {
    LongVector.Builder result = LongVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendLong(DateExtract.process(valueVector.getLong(p), chronoField, zone));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "DateExtractConstantEvaluator[" + "value=" + value + ", chronoField=" + chronoField + ", zone=" + zone + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(value);
  }
}
