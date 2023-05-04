// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateFormat}.
 * This class is generated. Do not edit it.
 */
public final class DateFormatEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  private final EvalOperator.ExpressionEvaluator formatter;

  public DateFormatEvaluator(EvalOperator.ExpressionEvaluator val,
      EvalOperator.ExpressionEvaluator formatter) {
    this.val = val;
    this.formatter = formatter;
  }

  @Override
  public Block eval(Page page) {
    Block valUncastBlock = val.eval(page);
    if (valUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    LongBlock valBlock = (LongBlock) valUncastBlock;
    Block formatterUncastBlock = formatter.eval(page);
    if (formatterUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock formatterBlock = (BytesRefBlock) formatterUncastBlock;
    LongVector valVector = valBlock.asVector();
    if (valVector == null) {
      return eval(page.getPositionCount(), valBlock, formatterBlock);
    }
    BytesRefVector formatterVector = formatterBlock.asVector();
    if (formatterVector == null) {
      return eval(page.getPositionCount(), valBlock, formatterBlock);
    }
    return eval(page.getPositionCount(), valVector, formatterVector).asBlock();
  }

  public BytesRefBlock eval(int positionCount, LongBlock valBlock, BytesRefBlock formatterBlock) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
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
      result.appendBytesRef(DateFormat.process(valBlock.getLong(valBlock.getFirstValueIndex(p)), formatterBlock.getBytesRef(formatterBlock.getFirstValueIndex(p), formatterScratch)));
    }
    return result.build();
  }

  public BytesRefVector eval(int positionCount, LongVector valVector,
      BytesRefVector formatterVector) {
    BytesRefVector.Builder result = BytesRefVector.newVectorBuilder(positionCount);
    BytesRef formatterScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBytesRef(DateFormat.process(valVector.getLong(p), formatterVector.getBytesRef(p, formatterScratch)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "DateFormatEvaluator[" + "val=" + val + ", formatter=" + formatter + "]";
  }
}
