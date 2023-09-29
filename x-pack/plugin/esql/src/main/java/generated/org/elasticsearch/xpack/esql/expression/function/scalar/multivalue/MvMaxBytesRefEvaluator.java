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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMax}.
 * This class is generated. Do not edit it.
 */
public final class MvMaxBytesRefEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  private final DriverContext driverContext;

  public MvMaxBytesRefEvaluator(EvalOperator.ExpressionEvaluator field,
      DriverContext driverContext) {
    super(field);
    this.driverContext = driverContext;
  }

  @Override
  public String name() {
    return "MvMax";
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block.Ref evalNullable(Block.Ref ref) {
    if (ref.block().mvSortedAscending()) {
      return evalAscendingNullable(ref);
    }
    try (ref) {
      BytesRefBlock v = (BytesRefBlock) ref.block();
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
          MvMax.process(value, next);
        }
        BytesRef result = value;
        builder.appendBytesRef(result);
      }
      return Block.Ref.floating(builder.build());
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block.Ref evalNotNullable(Block.Ref ref) {
    if (ref.block().mvSortedAscending()) {
      return evalAscendingNotNullable(ref);
    }
    try (ref) {
      BytesRefBlock v = (BytesRefBlock) ref.block();
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
          MvMax.process(value, next);
        }
        BytesRef result = value;
        builder.appendBytesRef(result);
      }
      return Block.Ref.floating(builder.build().asBlock());
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block.Ref evalAscendingNullable(Block.Ref ref) {
    try (ref) {
      BytesRefBlock v = (BytesRefBlock) ref.block();
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
        int idx = MvMax.ascendingIndex(valueCount);
        BytesRef result = v.getBytesRef(first + idx, firstScratch);
        builder.appendBytesRef(result);
      }
      return Block.Ref.floating(builder.build());
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block.Ref evalAscendingNotNullable(Block.Ref ref) {
    try (ref) {
      BytesRefBlock v = (BytesRefBlock) ref.block();
      int positionCount = v.getPositionCount();
      BytesRefVector.Builder builder = BytesRefVector.newVectorBuilder(positionCount, driverContext.blockFactory());
      BytesRef firstScratch = new BytesRef();
      BytesRef nextScratch = new BytesRef();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        int idx = MvMax.ascendingIndex(valueCount);
        BytesRef result = v.getBytesRef(first + idx, firstScratch);
        builder.appendBytesRef(result);
      }
      return Block.Ref.floating(builder.build().asBlock());
    }
  }
}
