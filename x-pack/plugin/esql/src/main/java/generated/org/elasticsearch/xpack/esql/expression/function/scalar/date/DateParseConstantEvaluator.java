// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateParse}.
 * This class is generated. Do not edit it.
 */
public final class DateParseConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  private final DateFormatter formatter;

  public DateParseConstantEvaluator(EvalOperator.ExpressionEvaluator val, DateFormatter formatter) {
    this.val = val;
    this.formatter = formatter;
  }

  @Override
  public Block eval(Page page) {
    Block valUncastBlock = val.eval(page);
    if (valUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock valBlock = (BytesRefBlock) valUncastBlock;
    BytesRefVector valVector = valBlock.asVector();
    if (valVector == null) {
      return eval(page.getPositionCount(), valBlock);
    }
    return eval(page.getPositionCount(), valVector);
  }

  public LongBlock eval(int positionCount, BytesRefBlock valBlock) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    BytesRef valScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (valBlock.isNull(p) || valBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      try {
        result.appendLong(DateParse.process(valBlock.getBytesRef(valBlock.getFirstValueIndex(p), valScratch), formatter));
      } catch (IllegalArgumentException e) {
        result.appendNull();
      }
    }
    return result.build();
  }

  public LongBlock eval(int positionCount, BytesRefVector valVector) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    BytesRef valScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      try {
        result.appendLong(DateParse.process(valVector.getBytesRef(p, valScratch), formatter));
      } catch (IllegalArgumentException e) {
        result.appendNull();
      }
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "DateParseConstantEvaluator[" + "val=" + val + ", formatter=" + formatter + "]";
  }
}
