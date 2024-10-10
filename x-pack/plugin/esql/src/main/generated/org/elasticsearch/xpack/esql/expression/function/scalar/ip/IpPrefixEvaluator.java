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

  private final EvalOperator.ExpressionEvaluator prefixLengthV4;

  private final EvalOperator.ExpressionEvaluator prefixLengthV6;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  public IpPrefixEvaluator(Source source, EvalOperator.ExpressionEvaluator ip,
      EvalOperator.ExpressionEvaluator prefixLengthV4,
      EvalOperator.ExpressionEvaluator prefixLengthV6, BytesRef scratch,
      DriverContext driverContext) {
    this.ip = ip;
    this.prefixLengthV4 = prefixLengthV4;
    this.prefixLengthV6 = prefixLengthV6;
    this.scratch = scratch;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock ipBlock = (BytesRefBlock) ip.eval(page)) {
      try (IntBlock prefixLengthV4Block = (IntBlock) prefixLengthV4.eval(page)) {
        try (IntBlock prefixLengthV6Block = (IntBlock) prefixLengthV6.eval(page)) {
          BytesRefVector ipVector = ipBlock.asVector();
          if (ipVector == null) {
            return eval(page.getPositionCount(), ipBlock, prefixLengthV4Block, prefixLengthV6Block);
          }
          IntVector prefixLengthV4Vector = prefixLengthV4Block.asVector();
          if (prefixLengthV4Vector == null) {
            return eval(page.getPositionCount(), ipBlock, prefixLengthV4Block, prefixLengthV6Block);
          }
          IntVector prefixLengthV6Vector = prefixLengthV6Block.asVector();
          if (prefixLengthV6Vector == null) {
            return eval(page.getPositionCount(), ipBlock, prefixLengthV4Block, prefixLengthV6Block);
          }
          return eval(page.getPositionCount(), ipVector, prefixLengthV4Vector, prefixLengthV6Vector);
        }
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock ipBlock, IntBlock prefixLengthV4Block,
      IntBlock prefixLengthV6Block) {
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
        if (prefixLengthV6Block.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (prefixLengthV6Block.getValueCount(p) != 1) {
          if (prefixLengthV6Block.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendBytesRef(IpPrefix.process(ipBlock.getBytesRef(ipBlock.getFirstValueIndex(p), ipScratch), prefixLengthV4Block.getInt(prefixLengthV4Block.getFirstValueIndex(p)), prefixLengthV6Block.getInt(prefixLengthV6Block.getFirstValueIndex(p)), this.scratch));
        } catch (IllegalArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector ipVector,
      IntVector prefixLengthV4Vector, IntVector prefixLengthV6Vector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef ipScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBytesRef(IpPrefix.process(ipVector.getBytesRef(p, ipScratch), prefixLengthV4Vector.getInt(p), prefixLengthV6Vector.getInt(p), this.scratch));
        } catch (IllegalArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "IpPrefixEvaluator[" + "ip=" + ip + ", prefixLengthV4=" + prefixLengthV4 + ", prefixLengthV6=" + prefixLengthV6 + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(ip, prefixLengthV4, prefixLengthV6);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory ip;

    private final EvalOperator.ExpressionEvaluator.Factory prefixLengthV4;

    private final EvalOperator.ExpressionEvaluator.Factory prefixLengthV6;

    private final Function<DriverContext, BytesRef> scratch;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory ip,
        EvalOperator.ExpressionEvaluator.Factory prefixLengthV4,
        EvalOperator.ExpressionEvaluator.Factory prefixLengthV6,
        Function<DriverContext, BytesRef> scratch) {
      this.source = source;
      this.ip = ip;
      this.prefixLengthV4 = prefixLengthV4;
      this.prefixLengthV6 = prefixLengthV6;
      this.scratch = scratch;
    }

    @Override
    public IpPrefixEvaluator get(DriverContext context) {
      return new IpPrefixEvaluator(source, ip.get(context), prefixLengthV4.get(context), prefixLengthV6.get(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "IpPrefixEvaluator[" + "ip=" + ip + ", prefixLengthV4=" + prefixLengthV4 + ", prefixLengthV6=" + prefixLengthV6 + "]";
    }
  }
}
