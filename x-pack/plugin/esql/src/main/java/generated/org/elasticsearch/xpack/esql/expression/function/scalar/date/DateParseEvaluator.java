// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.time.ZoneId;
import org.apache.lucene.util.BytesRef;
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
public final class DateParseEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  private final EvalOperator.ExpressionEvaluator formatter;

  private final ZoneId zoneId;

  public DateParseEvaluator(EvalOperator.ExpressionEvaluator val,
      EvalOperator.ExpressionEvaluator formatter, ZoneId zoneId) {
    this.val = val;
    this.formatter = formatter;
    this.zoneId = zoneId;
  }

  @Override
  public Block eval(Page page) {
    Block valUncastBlock = val.eval(page);
    if (valUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock valBlock = (BytesRefBlock) valUncastBlock;
    Block formatterUncastBlock = formatter.eval(page);
    if (formatterUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock formatterBlock = (BytesRefBlock) formatterUncastBlock;
    BytesRefVector valVector = valBlock.asVector();
    if (valVector == null) {
      return eval(page.getPositionCount(), valBlock, formatterBlock);
    }
    BytesRefVector formatterVector = formatterBlock.asVector();
    if (formatterVector == null) {
      return eval(page.getPositionCount(), valBlock, formatterBlock);
    }
    return eval(page.getPositionCount(), valVector, formatterVector);
  }

  public LongBlock eval(int positionCount, BytesRefBlock valBlock, BytesRefBlock formatterBlock) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    BytesRef valScratch = new BytesRef();
    BytesRef formatterScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (valBlock.isNull(p) || valBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (formatterBlock.isNull(p) || formatterBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      try {
        result.appendLong(DateParse.process(valBlock.getBytesRef(valBlock.getFirstValueIndex(p), valScratch), formatterBlock.getBytesRef(formatterBlock.getFirstValueIndex(p), formatterScratch), zoneId));
      } catch (IllegalArgumentException e) {
        result.appendNull();
      }
    }
    return result.build();
  }

  public LongBlock eval(int positionCount, BytesRefVector valVector,
      BytesRefVector formatterVector) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    BytesRef valScratch = new BytesRef();
    BytesRef formatterScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      try {
        result.appendLong(DateParse.process(valVector.getBytesRef(p, valScratch), formatterVector.getBytesRef(p, formatterScratch), zoneId));
      } catch (IllegalArgumentException e) {
        result.appendNull();
      }
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "DateParseEvaluator[" + "val=" + val + ", formatter=" + formatter + ", zoneId=" + zoneId + "]";
  }
}
