// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link TopSnippets}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class TopSnippetsBytesRefEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TopSnippetsBytesRefEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator query;

  private final int numSnippets;

  private final int numWords;

  private final DriverContext driverContext;

  private Warnings warnings;

  public TopSnippetsBytesRefEvaluator(Source source, EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator query, int numSnippets, int numWords,
      DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.query = query;
    this.numSnippets = numSnippets;
    this.numWords = numWords;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (BytesRefBlock queryBlock = (BytesRefBlock) query.eval(page)) {
        BytesRefVector strVector = strBlock.asVector();
        if (strVector == null) {
          return eval(page.getPositionCount(), strBlock, queryBlock);
        }
        BytesRefVector queryVector = queryBlock.asVector();
        if (queryVector == null) {
          return eval(page.getPositionCount(), strBlock, queryBlock);
        }
        return eval(page.getPositionCount(), strVector, queryVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += str.baseRamBytesUsed();
    baseRamBytesUsed += query.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock queryBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef queryScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (strBlock.getValueCount(p)) {
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
        switch (queryBlock.getValueCount(p)) {
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
        BytesRef str = strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch);
        BytesRef query = queryBlock.getBytesRef(queryBlock.getFirstValueIndex(p), queryScratch);
        TopSnippets.process(result, str, query, this.numSnippets, this.numWords);
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector strVector,
      BytesRefVector queryVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef queryScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef str = strVector.getBytesRef(p, strScratch);
        BytesRef query = queryVector.getBytesRef(p, queryScratch);
        TopSnippets.process(result, str, query, this.numSnippets, this.numWords);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "TopSnippetsBytesRefEvaluator[" + "str=" + str + ", query=" + query + ", numSnippets=" + numSnippets + ", numWords=" + numWords + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, query);
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

    private final EvalOperator.ExpressionEvaluator.Factory str;

    private final EvalOperator.ExpressionEvaluator.Factory query;

    private final int numSnippets;

    private final int numWords;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory query, int numSnippets, int numWords) {
      this.source = source;
      this.str = str;
      this.query = query;
      this.numSnippets = numSnippets;
      this.numWords = numWords;
    }

    @Override
    public TopSnippetsBytesRefEvaluator get(DriverContext context) {
      return new TopSnippetsBytesRefEvaluator(source, str.get(context), query.get(context), numSnippets, numWords, context);
    }

    @Override
    public String toString() {
      return "TopSnippetsBytesRefEvaluator[" + "str=" + str + ", query=" + query + ", numSnippets=" + numSnippets + ", numWords=" + numWords + "]";
    }
  }
}
