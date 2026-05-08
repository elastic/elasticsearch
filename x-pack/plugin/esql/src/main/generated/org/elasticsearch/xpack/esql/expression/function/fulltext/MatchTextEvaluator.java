// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.fulltext;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Match}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class MatchTextEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MatchTextEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator value;

  private final String queryString;

  private final Analyzer analyzer;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MatchTextEvaluator(Source source, ExpressionEvaluator value, String queryString,
      Analyzer analyzer, DriverContext driverContext) {
    this.source = source;
    this.value = value;
    this.queryString = queryString;
    this.analyzer = analyzer;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock valueBlock = (BytesRefBlock) value.eval(page)) {
      BytesRefVector valueVector = valueBlock.asVector();
      if (valueVector == null) {
        return eval(page.getPositionCount(), valueBlock);
      }
      return eval(page.getPositionCount(), valueVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += value.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock valueBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef valueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (valueBlock.getValueCount(p)) {
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
        BytesRef value = valueBlock.getBytesRef(valueBlock.getFirstValueIndex(p), valueScratch);
        try {
          result.appendBoolean(Match.processTextKeyword(value, this.queryString, this.analyzer));
        } catch (IOException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefVector valueVector) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef valueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef value = valueVector.getBytesRef(p, valueScratch);
        try {
          result.appendBoolean(Match.processTextKeyword(value, this.queryString, this.analyzer));
        } catch (IOException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MatchTextEvaluator[" + "value=" + value + ", queryString=" + queryString + ", analyzer=" + analyzer + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(value);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory value;

    private final String queryString;

    private final Analyzer analyzer;

    public Factory(Source source, ExpressionEvaluator.Factory value, String queryString,
        Analyzer analyzer) {
      this.source = source;
      this.value = value;
      this.queryString = queryString;
      this.analyzer = analyzer;
    }

    @Override
    public MatchTextEvaluator get(DriverContext context) {
      return new MatchTextEvaluator(source, value.get(context), queryString, analyzer, context);
    }

    @Override
    public String toString() {
      return "MatchTextEvaluator[" + "value=" + value + ", queryString=" + queryString + ", analyzer=" + analyzer + "]";
    }
  }
}
