// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.NullPointerException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link UriParts}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class UriPartsEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(UriPartsEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator string;

  private final EvalOperator.ExpressionEvaluator component;

  private final DriverContext driverContext;

  private Warnings warnings;

  public UriPartsEvaluator(Source source, EvalOperator.ExpressionEvaluator string,
      EvalOperator.ExpressionEvaluator component, DriverContext driverContext) {
    this.source = source;
    this.string = string;
    this.component = component;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock stringBlock = (BytesRefBlock) string.eval(page)) {
      try (BytesRefBlock componentBlock = (BytesRefBlock) component.eval(page)) {
        BytesRefVector stringVector = stringBlock.asVector();
        if (stringVector == null) {
          return eval(page.getPositionCount(), stringBlock, componentBlock);
        }
        BytesRefVector componentVector = componentBlock.asVector();
        if (componentVector == null) {
          return eval(page.getPositionCount(), stringBlock, componentBlock);
        }
        return eval(page.getPositionCount(), stringVector, componentVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += string.baseRamBytesUsed();
    baseRamBytesUsed += component.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock stringBlock,
      BytesRefBlock componentBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef stringScratch = new BytesRef();
      BytesRef componentScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (stringBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        switch (componentBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        BytesRef string = stringBlock.getBytesRef(stringBlock.getFirstValueIndex(p), stringScratch);
        BytesRef component = componentBlock.getBytesRef(componentBlock.getFirstValueIndex(p), componentScratch);
        try {
          result.appendBytesRef(UriParts.process(string, component));
        } catch (NullPointerException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector stringVector,
      BytesRefVector componentVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef stringScratch = new BytesRef();
      BytesRef componentScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef string = stringVector.getBytesRef(p, stringScratch);
        BytesRef component = componentVector.getBytesRef(p, componentScratch);
        try {
          result.appendBytesRef(UriParts.process(string, component));
        } catch (NullPointerException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "UriPartsEvaluator[" + "string=" + string + ", component=" + component + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(string, component);
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

    private final EvalOperator.ExpressionEvaluator.Factory string;

    private final EvalOperator.ExpressionEvaluator.Factory component;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory string,
        EvalOperator.ExpressionEvaluator.Factory component) {
      this.source = source;
      this.string = string;
      this.component = component;
    }

    @Override
    public UriPartsEvaluator get(DriverContext context) {
      return new UriPartsEvaluator(source, string.get(context), component.get(context), context);
    }

    @Override
    public String toString() {
      return "UriPartsEvaluator[" + "string=" + string + ", component=" + component + "]";
    }
  }
}
