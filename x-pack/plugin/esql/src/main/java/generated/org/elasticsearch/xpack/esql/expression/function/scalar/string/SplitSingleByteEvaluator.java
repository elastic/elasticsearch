// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Split}.
 * This class is generated. Do not edit it.
 */
public final class SplitSingleByteEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final byte delim;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  public SplitSingleByteEvaluator(EvalOperator.ExpressionEvaluator str, byte delim,
      BytesRef scratch, DriverContext driverContext) {
    this.str = str;
    this.delim = delim;
    this.scratch = scratch;
    this.driverContext = driverContext;
  }

  @Override
  public Block.Ref eval(Page page) {
    try (Block.Ref strRef = str.eval(page)) {
      if (strRef.block().areAllValuesNull()) {
        return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
      }
      BytesRefBlock strBlock = (BytesRefBlock) strRef.block();
      BytesRefVector strVector = strBlock.asVector();
      if (strVector == null) {
        return Block.Ref.floating(eval(page.getPositionCount(), strBlock));
      }
      return Block.Ref.floating(eval(page.getPositionCount(), strVector));
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (strBlock.isNull(p) || strBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      Split.process(result, strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), delim, scratch);
    }
    return result.build();
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector strVector) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      Split.process(result, strVector.getBytesRef(p, strScratch), delim, scratch);
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "SplitSingleByteEvaluator[" + "str=" + str + ", delim=" + delim + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str);
  }
}
