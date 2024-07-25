// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link IpPrefix}.
 * This class is generated. Do not edit it.
 */
public final class IpPrefixOnlyV4Evaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator ip;

  private final EvalOperator.ExpressionEvaluator prefixLengthV4;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  public IpPrefixOnlyV4Evaluator(Source source, EvalOperator.ExpressionEvaluator ip,
      EvalOperator.ExpressionEvaluator prefixLengthV4, BytesRef scratch,
      DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.ip = ip;
    this.prefixLengthV4 = prefixLengthV4;
    this.scratch = scratch;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock ipBlock = (BytesRefBlock) ip.eval(page)) {
      try (IntBlock prefixLengthV4Block = (IntBlock) prefixLengthV4.eval(page)) {
        BytesRefVector ipVector = ipBlock.asVector();
        if (ipVector == null) {
          return eval(page.getPositionCount(), ipBlock, prefixLengthV4Block);
        }
        IntVector prefixLengthV4Vector = prefixLengthV4Block.asVector();
        if (prefixLengthV4Vector == null) {
          return eval(page.getPositionCount(), ipBlock, prefixLengthV4Block);
        }
        return eval(page.getPositionCount(), ipVector, prefixLengthV4Vector).asBlock();
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock ipBlock,
      IntBlock prefixLengthV4Block) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef ipScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (ipBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (ipBlock.getValueCount(p) != 1) {
          if (ipBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (prefixLengthV4Block.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (prefixLengthV4Block.getValueCount(p) != 1) {
          if (prefixLengthV4Block.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBytesRef(IpPrefix.process(ipBlock.getBytesRef(ipBlock.getFirstValueIndex(p), ipScratch), prefixLengthV4Block.getInt(prefixLengthV4Block.getFirstValueIndex(p)), scratch));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector ipVector,
      IntVector prefixLengthV4Vector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef ipScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(IpPrefix.process(ipVector.getBytesRef(p, ipScratch), prefixLengthV4Vector.getInt(p), scratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "IpPrefixOnlyV4Evaluator[" + "ip=" + ip + ", prefixLengthV4=" + prefixLengthV4 + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(ip, prefixLengthV4);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory ip;

    private final EvalOperator.ExpressionEvaluator.Factory prefixLengthV4;

    private final Function<DriverContext, BytesRef> scratch;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory ip,
        EvalOperator.ExpressionEvaluator.Factory prefixLengthV4,
        Function<DriverContext, BytesRef> scratch) {
      this.source = source;
      this.ip = ip;
      this.prefixLengthV4 = prefixLengthV4;
      this.scratch = scratch;
    }

    @Override
    public IpPrefixOnlyV4Evaluator get(DriverContext context) {
      return new IpPrefixOnlyV4Evaluator(source, ip.get(context), prefixLengthV4.get(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "IpPrefixOnlyV4Evaluator[" + "ip=" + ip + ", prefixLengthV4=" + prefixLengthV4 + "]";
    }
  }
}
