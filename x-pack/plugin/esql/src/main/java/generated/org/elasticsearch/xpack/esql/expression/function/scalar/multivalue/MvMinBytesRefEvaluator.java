// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefArrayVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMin}.
 * This class is generated. Do not edit it.
 */
public final class MvMinBytesRefEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvMinBytesRefEvaluator(EvalOperator.ExpressionEvaluator field) {
    super(field);
  }

  @Override
  public String name() {
    return "MvMin";
  }

  @Override
  public Block evalNullable(Block fieldVal) {
    BytesRefBlock v = (BytesRefBlock) fieldVal;
    int positionCount = v.getPositionCount();
    BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef firstScratch = new BytesRef();
    BytesRef nextScratch = new BytesRef();
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      if (valueCount == 0) {
        builder.appendNull();
        continue;
      }
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      BytesRef value = v.getBytesRef(first, firstScratch);
      for (int i = first + 1; i < end; i++) {
        BytesRef next = v.getBytesRef(i, nextScratch);
        MvMin.process(value, next);
      }
      BytesRef result = value;
      builder.appendBytesRef(result);
    }
    return builder.build();
  }

  @Override
  public Vector evalNotNullable(Block fieldVal) {
    BytesRefBlock v = (BytesRefBlock) fieldVal;
    int positionCount = v.getPositionCount();
    BytesRefArray values = new BytesRefArray(positionCount, BigArrays.NON_RECYCLING_INSTANCE);
    BytesRef firstScratch = new BytesRef();
    BytesRef nextScratch = new BytesRef();
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      BytesRef value = v.getBytesRef(first, firstScratch);
      for (int i = first + 1; i < end; i++) {
        BytesRef next = v.getBytesRef(i, nextScratch);
        MvMin.process(value, next);
      }
      BytesRef result = value;
      values.append(result);
    }
    return new BytesRefArrayVector(values, positionCount);
  }
}
