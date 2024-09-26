// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Reverse}.
 * This class is generated. Do not edit it.
 */
public final class ReverseEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator ref;

  private final DriverContext driverContext;

  public ReverseEvaluator(Source source, EvalOperator.ExpressionEvaluator ref,
      DriverContext driverContext) {
    this.ref = ref;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock refBlock = (BytesRefBlock) ref.eval(page)) {
      BytesRefVector refVector = refBlock.asVector();
      if (refVector == null) {
        return eval(page.getPositionCount(), refBlock);
      }
      return eval(page.getPositionCount(), refVector).asBlock();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock refBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef refScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (refBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (refBlock.getValueCount(p) != 1) {
          if (refBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBytesRef(Reverse.process(refBlock.getBytesRef(refBlock.getFirstValueIndex(p), refScratch)));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector refVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef refScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(Reverse.process(refVector.getBytesRef(p, refScratch)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ReverseEvaluator[" + "ref=" + ref + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(ref);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory ref;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory ref) {
      this.source = source;
      this.ref = ref;
    }

    @Override
    public ReverseEvaluator get(DriverContext context) {
      return new ReverseEvaluator(source, ref.get(context), context);
    }

    @Override
    public String toString() {
      return "ReverseEvaluator[" + "ref=" + ref + "]";
    }
  }
}
