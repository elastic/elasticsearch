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

  private final EvalOperator.ExpressionEvaluator urlString;

  private final EvalOperator.ExpressionEvaluator field;

  private final DriverContext driverContext;

  private Warnings warnings;

  public UriPartsEvaluator(Source source, EvalOperator.ExpressionEvaluator urlString,
      EvalOperator.ExpressionEvaluator field, DriverContext driverContext) {
    this.source = source;
    this.urlString = urlString;
    this.field = field;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock urlStringBlock = (BytesRefBlock) urlString.eval(page)) {
      try (BytesRefBlock fieldBlock = (BytesRefBlock) field.eval(page)) {
        BytesRefVector urlStringVector = urlStringBlock.asVector();
        if (urlStringVector == null) {
          return eval(page.getPositionCount(), urlStringBlock, fieldBlock);
        }
        BytesRefVector fieldVector = fieldBlock.asVector();
        if (fieldVector == null) {
          return eval(page.getPositionCount(), urlStringBlock, fieldBlock);
        }
        return eval(page.getPositionCount(), urlStringVector, fieldVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += urlString.baseRamBytesUsed();
    baseRamBytesUsed += field.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock urlStringBlock,
      BytesRefBlock fieldBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef urlStringScratch = new BytesRef();
      BytesRef fieldScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (urlStringBlock.getValueCount(p)) {
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
        switch (fieldBlock.getValueCount(p)) {
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
        BytesRef urlString = urlStringBlock.getBytesRef(urlStringBlock.getFirstValueIndex(p), urlStringScratch);
        BytesRef field = fieldBlock.getBytesRef(fieldBlock.getFirstValueIndex(p), fieldScratch);
        try {
          result.appendBytesRef(UriParts.process(urlString, field));
        } catch (NullPointerException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector urlStringVector,
      BytesRefVector fieldVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef urlStringScratch = new BytesRef();
      BytesRef fieldScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef urlString = urlStringVector.getBytesRef(p, urlStringScratch);
        BytesRef field = fieldVector.getBytesRef(p, fieldScratch);
        try {
          result.appendBytesRef(UriParts.process(urlString, field));
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
    return "UriPartsEvaluator[" + "urlString=" + urlString + ", field=" + field + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(urlString, field);
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

    private final EvalOperator.ExpressionEvaluator.Factory urlString;

    private final EvalOperator.ExpressionEvaluator.Factory field;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory urlString,
        EvalOperator.ExpressionEvaluator.Factory field) {
      this.source = source;
      this.urlString = urlString;
      this.field = field;
    }

    @Override
    public UriPartsEvaluator get(DriverContext context) {
      return new UriPartsEvaluator(source, urlString.get(context), field.get(context), context);
    }

    @Override
    public String toString() {
      return "UriPartsEvaluator[" + "urlString=" + urlString + ", field=" + field + "]";
    }
  }
}
