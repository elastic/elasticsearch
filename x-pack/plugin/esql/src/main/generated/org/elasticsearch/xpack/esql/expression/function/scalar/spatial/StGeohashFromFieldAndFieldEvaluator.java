// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StGeohash}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StGeohashFromFieldAndFieldEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator in;

  private final EvalOperator.ExpressionEvaluator precision;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StGeohashFromFieldAndFieldEvaluator(Source source, EvalOperator.ExpressionEvaluator in,
      EvalOperator.ExpressionEvaluator precision, DriverContext driverContext) {
    this.source = source;
    this.in = in;
    this.precision = precision;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock inBlock = (BytesRefBlock) in.eval(page)) {
      try (IntBlock precisionBlock = (IntBlock) precision.eval(page)) {
        BytesRefVector inVector = inBlock.asVector();
        if (inVector == null) {
          return eval(page.getPositionCount(), inBlock, precisionBlock);
        }
        IntVector precisionVector = precisionBlock.asVector();
        if (precisionVector == null) {
          return eval(page.getPositionCount(), inBlock, precisionBlock);
        }
        return eval(page.getPositionCount(), inVector, precisionVector);
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock inBlock, IntBlock precisionBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef inScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (inBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (inBlock.getValueCount(p) != 1) {
          if (inBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (precisionBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (precisionBlock.getValueCount(p) != 1) {
          if (precisionBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendBytesRef(StGeohash.fromFieldAndField(inBlock.getBytesRef(inBlock.getFirstValueIndex(p), inScratch), precisionBlock.getInt(precisionBlock.getFirstValueIndex(p))));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector inVector, IntVector precisionVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef inScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBytesRef(StGeohash.fromFieldAndField(inVector.getBytesRef(p, inScratch), precisionVector.getInt(p)));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StGeohashFromFieldAndFieldEvaluator[" + "in=" + in + ", precision=" + precision + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(in, precision);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(
              driverContext.warningsMode(),
              source.source().getLineNumber(),
              source.source().getColumnNumber(),
              source.text()
          );
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory in;

    private final EvalOperator.ExpressionEvaluator.Factory precision;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory in,
        EvalOperator.ExpressionEvaluator.Factory precision) {
      this.source = source;
      this.in = in;
      this.precision = precision;
    }

    @Override
    public StGeohashFromFieldAndFieldEvaluator get(DriverContext context) {
      return new StGeohashFromFieldAndFieldEvaluator(source, in.get(context), precision.get(context), context);
    }

    @Override
    public String toString() {
      return "StGeohashFromFieldAndFieldEvaluator[" + "in=" + in + ", precision=" + precision + "]";
    }
  }
}
