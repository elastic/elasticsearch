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
public final class IpPrefixEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator ip;

  private final EvalOperator.ExpressionEvaluator prefixLength;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  public IpPrefixEvaluator(Source source, EvalOperator.ExpressionEvaluator ip,
      EvalOperator.ExpressionEvaluator prefixLength, BytesRef scratch,
      DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.ip = ip;
    this.prefixLength = prefixLength;
    this.scratch = scratch;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock ipBlock = (BytesRefBlock) ip.eval(page)) {
      try (IntBlock prefixLengthBlock = (IntBlock) prefixLength.eval(page)) {
        BytesRefVector ipVector = ipBlock.asVector();
        if (ipVector == null) {
          return eval(page.getPositionCount(), ipBlock, prefixLengthBlock);
        }
        IntVector prefixLengthVector = prefixLengthBlock.asVector();
        if (prefixLengthVector == null) {
          return eval(page.getPositionCount(), ipBlock, prefixLengthBlock);
        }
        return eval(page.getPositionCount(), ipVector, prefixLengthVector).asBlock();
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock ipBlock, IntBlock prefixLengthBlock) {
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
        if (prefixLengthBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (prefixLengthBlock.getValueCount(p) != 1) {
          if (prefixLengthBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBytesRef(IpPrefix.process(ipBlock.getBytesRef(ipBlock.getFirstValueIndex(p), ipScratch), prefixLengthBlock.getInt(prefixLengthBlock.getFirstValueIndex(p)), scratch));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector ipVector,
      IntVector prefixLengthVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef ipScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(IpPrefix.process(ipVector.getBytesRef(p, ipScratch), prefixLengthVector.getInt(p), scratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "IpPrefixEvaluator[" + "ip=" + ip + ", prefixLength=" + prefixLength + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(ip, prefixLength);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory ip;

    private final EvalOperator.ExpressionEvaluator.Factory prefixLength;

    private final Function<DriverContext, BytesRef> scratch;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory ip,
        EvalOperator.ExpressionEvaluator.Factory prefixLength,
        Function<DriverContext, BytesRef> scratch) {
      this.source = source;
      this.ip = ip;
      this.prefixLength = prefixLength;
      this.scratch = scratch;
    }

    @Override
    public IpPrefixEvaluator get(DriverContext context) {
      return new IpPrefixEvaluator(source, ip.get(context), prefixLength.get(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "IpPrefixEvaluator[" + "ip=" + ip + ", prefixLength=" + prefixLength + "]";
    }
  }
}
