// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link EndsWith}.
 * This class is generated. Do not edit it.
 */
public final class EndsWithEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator suffix;

  private final DriverContext driverContext;

  public EndsWithEvaluator(EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator suffix, DriverContext driverContext) {
    this.str = str;
    this.suffix = suffix;
    this.driverContext = driverContext;
  }

  @Override
  public Block.Ref eval(Page page) {
    try (Block.Ref strRef = str.eval(page)) {
      if (strRef.block().areAllValuesNull()) {
        return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
      }
      BytesRefBlock strBlock = (BytesRefBlock) strRef.block();
      try (Block.Ref suffixRef = suffix.eval(page)) {
        if (suffixRef.block().areAllValuesNull()) {
          return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
        }
        BytesRefBlock suffixBlock = (BytesRefBlock) suffixRef.block();
        BytesRefVector strVector = strBlock.asVector();
        if (strVector == null) {
          return Block.Ref.floating(eval(page.getPositionCount(), strBlock, suffixBlock));
        }
        BytesRefVector suffixVector = suffixBlock.asVector();
        if (suffixVector == null) {
          return Block.Ref.floating(eval(page.getPositionCount(), strBlock, suffixBlock));
        }
        return Block.Ref.floating(eval(page.getPositionCount(), strVector, suffixVector).asBlock());
      }
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock suffixBlock) {
    BooleanBlock.Builder result = BooleanBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    BytesRef suffixScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (strBlock.isNull(p) || strBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (suffixBlock.isNull(p) || suffixBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendBoolean(EndsWith.process(strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), suffixBlock.getBytesRef(suffixBlock.getFirstValueIndex(p), suffixScratch)));
    }
    return result.build();
  }

  public BooleanVector eval(int positionCount, BytesRefVector strVector,
      BytesRefVector suffixVector) {
    BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    BytesRef suffixScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBoolean(EndsWith.process(strVector.getBytesRef(p, strScratch), suffixVector.getBytesRef(p, suffixScratch)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "EndsWithEvaluator[" + "str=" + str + ", suffix=" + suffix + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, suffix);
  }
}
