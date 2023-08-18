// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link CIDRMatch}.
 * This class is generated. Do not edit it.
 */
public final class CIDRMatchEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator ip;

  private final EvalOperator.ExpressionEvaluator[] cidrs;

  public CIDRMatchEvaluator(EvalOperator.ExpressionEvaluator ip,
      EvalOperator.ExpressionEvaluator[] cidrs) {
    this.ip = ip;
    this.cidrs = cidrs;
  }

  @Override
  public Block eval(Page page) {
    Block ipUncastBlock = ip.eval(page);
    if (ipUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock ipBlock = (BytesRefBlock) ipUncastBlock;
    BytesRefBlock[] cidrsBlocks = new BytesRefBlock[cidrs.length];
    for (int i = 0; i < cidrsBlocks.length; i++) {
      Block block = cidrs[i].eval(page);
      if (block.areAllValuesNull()) {
        return Block.constantNullBlock(page.getPositionCount());
      }
      cidrsBlocks[i] = (BytesRefBlock) block;
    }
    BytesRefVector ipVector = ipBlock.asVector();
    if (ipVector == null) {
      return eval(page.getPositionCount(), ipBlock, cidrsBlocks);
    }
    BytesRefVector[] cidrsVectors = new BytesRefVector[cidrs.length];
    for (int i = 0; i < cidrsBlocks.length; i++) {
      cidrsVectors[i] = cidrsBlocks[i].asVector();
      if (cidrsVectors[i] == null) {
        return eval(page.getPositionCount(), ipBlock, cidrsBlocks);
      }
    }
    return eval(page.getPositionCount(), ipVector, cidrsVectors).asBlock();
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock ipBlock, BytesRefBlock[] cidrsBlocks) {
    BooleanBlock.Builder result = BooleanBlock.newBlockBuilder(positionCount);
    BytesRef ipScratch = new BytesRef();
    BytesRef[] cidrsValues = new BytesRef[cidrs.length];
    BytesRef[] cidrsScratch = new BytesRef[cidrs.length];
    for (int i = 0; i < cidrs.length; i++) {
      cidrsScratch[i] = new BytesRef();
    }
    position: for (int p = 0; p < positionCount; p++) {
      if (ipBlock.isNull(p) || ipBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      for (int i = 0; i < cidrsBlocks.length; i++) {
        if (cidrsBlocks[i].isNull(p) || cidrsBlocks[i].getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
      }
      // unpack cidrsBlocks into cidrsValues
      for (int i = 0; i < cidrsBlocks.length; i++) {
        int o = cidrsBlocks[i].getFirstValueIndex(p);
        cidrsValues[i] = cidrsBlocks[i].getBytesRef(o, cidrsScratch[i]);
      }
      result.appendBoolean(CIDRMatch.process(ipBlock.getBytesRef(ipBlock.getFirstValueIndex(p), ipScratch), cidrsValues));
    }
    return result.build();
  }

  public BooleanVector eval(int positionCount, BytesRefVector ipVector,
      BytesRefVector[] cidrsVectors) {
    BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
    BytesRef ipScratch = new BytesRef();
    BytesRef[] cidrsValues = new BytesRef[cidrs.length];
    BytesRef[] cidrsScratch = new BytesRef[cidrs.length];
    for (int i = 0; i < cidrs.length; i++) {
      cidrsScratch[i] = new BytesRef();
    }
    position: for (int p = 0; p < positionCount; p++) {
      // unpack cidrsVectors into cidrsValues
      for (int i = 0; i < cidrsVectors.length; i++) {
        cidrsValues[i] = cidrsVectors[i].getBytesRef(p, cidrsScratch[i]);
      }
      result.appendBoolean(CIDRMatch.process(ipVector.getBytesRef(p, ipScratch), cidrsValues));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "CIDRMatchEvaluator[" + "ip=" + ip + ", cidrs=" + Arrays.toString(cidrs) + "]";
  }
}
