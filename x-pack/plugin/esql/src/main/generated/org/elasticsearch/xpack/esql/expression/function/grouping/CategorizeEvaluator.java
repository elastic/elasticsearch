// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.grouping;

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
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Categorize}.
 * This class is generated. Do not edit it.
 */
public final class CategorizeEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator v;

  private final CategorizationAnalyzer analyzer;

  private final TokenListCategorizer.CloseableTokenListCategorizer categorizer;

  private final DriverContext driverContext;

  private Warnings warnings;

  public CategorizeEvaluator(Source source, EvalOperator.ExpressionEvaluator v,
      CategorizationAnalyzer analyzer,
      TokenListCategorizer.CloseableTokenListCategorizer categorizer, DriverContext driverContext) {
    this.source = source;
    this.v = v;
    this.analyzer = analyzer;
    this.categorizer = categorizer;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock vBlock = (BytesRefBlock) v.eval(page)) {
      BytesRefVector vVector = vBlock.asVector();
      if (vVector == null) {
        return eval(page.getPositionCount(), vBlock);
      }
      return eval(page.getPositionCount(), vVector).asBlock();
    }
  }

  public IntBlock eval(int positionCount, BytesRefBlock vBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      BytesRef vScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (vBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (vBlock.getValueCount(p) != 1) {
          if (vBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendInt(Categorize.process(vBlock.getBytesRef(vBlock.getFirstValueIndex(p), vScratch), this.analyzer, this.categorizer));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, BytesRefVector vVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      BytesRef vScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendInt(p, Categorize.process(vVector.getBytesRef(p, vScratch), this.analyzer, this.categorizer));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "CategorizeEvaluator[" + "v=" + v + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(v, analyzer, categorizer);
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

    private final EvalOperator.ExpressionEvaluator.Factory v;

    private final Function<DriverContext, CategorizationAnalyzer> analyzer;

    private final Function<DriverContext, TokenListCategorizer.CloseableTokenListCategorizer> categorizer;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory v,
        Function<DriverContext, CategorizationAnalyzer> analyzer,
        Function<DriverContext, TokenListCategorizer.CloseableTokenListCategorizer> categorizer) {
      this.source = source;
      this.v = v;
      this.analyzer = analyzer;
      this.categorizer = categorizer;
    }

    @Override
    public CategorizeEvaluator get(DriverContext context) {
      return new CategorizeEvaluator(source, v.get(context), analyzer.apply(context), categorizer.apply(context), context);
    }

    @Override
    public String toString() {
      return "CategorizeEvaluator[" + "v=" + v + "]";
    }
  }
}
