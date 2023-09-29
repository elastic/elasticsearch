// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMin}.
 * This class is generated. Do not edit it.
 */
public final class MvMinBytesRefEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  private final DriverContext driverContext;

  public MvMinBytesRefEvaluator(EvalOperator.ExpressionEvaluator field,
      DriverContext driverContext) {
    super(field);
    this.driverContext = driverContext;
  }

  @Override
  public String name() {
    return "MvMin";
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block evalNullable(Block fieldVal) {
    if (fieldVal.mvSortedAscending()) {
      return evalAscendingNullable(fieldVal);
    }
    BytesRefBlock v = (BytesRefBlock) fieldVal;
    int positionCount = v.getPositionCount();
    BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(positionCount, driverContext.blockFactory());
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

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Vector evalNotNullable(Block fieldVal) {
    if (fieldVal.mvSortedAscending()) {
      return evalAscendingNotNullable(fieldVal);
    }
    BytesRefBlock v = (BytesRefBlock) fieldVal;
    int positionCount = v.getPositionCount();
    BytesRefVector.Builder builder = BytesRefVector.newVectorBuilder(positionCount, driverContext.blockFactory());
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
      builder.appendBytesRef(result);
    }
    return builder.build();
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNullable(Block fieldVal) {
    BytesRefBlock v = (BytesRefBlock) fieldVal;
    int positionCount = v.getPositionCount();
    BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(positionCount, driverContext.blockFactory());
    BytesRef firstScratch = new BytesRef();
    BytesRef nextScratch = new BytesRef();
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      if (valueCount == 0) {
        builder.appendNull();
        continue;
      }
      int first = v.getFirstValueIndex(p);
      int idx = MvMin.ascendingIndex(valueCount);
      BytesRef result = v.getBytesRef(first + idx, firstScratch);
      builder.appendBytesRef(result);
    }
    return builder.build();
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Vector evalAscendingNotNullable(Block fieldVal) {
    BytesRefBlock v = (BytesRefBlock) fieldVal;
    int positionCount = v.getPositionCount();
    BytesRefVector.Builder builder = BytesRefVector.newVectorBuilder(positionCount, driverContext.blockFactory());
    BytesRef firstScratch = new BytesRef();
    BytesRef nextScratch = new BytesRef();
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      int first = v.getFirstValueIndex(p);
      int idx = MvMin.ascendingIndex(valueCount);
      BytesRef result = v.getBytesRef(first + idx, firstScratch);
      builder.appendBytesRef(result);
    }
    return builder.build();
  }
}
