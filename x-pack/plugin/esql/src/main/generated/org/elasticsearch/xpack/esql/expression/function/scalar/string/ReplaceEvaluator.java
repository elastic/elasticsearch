// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.regex.PatternSyntaxException;
import org.apache.lucene.util.BytesRef;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Replace}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ReplaceEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator regex;

  private final EvalOperator.ExpressionEvaluator newStr;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ReplaceEvaluator(Source source, EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator regex, EvalOperator.ExpressionEvaluator newStr,
      DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.regex = regex;
    this.newStr = newStr;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (BytesRefBlock regexBlock = (BytesRefBlock) regex.eval(page)) {
        try (BytesRefBlock newStrBlock = (BytesRefBlock) newStr.eval(page)) {
          BytesRefVector strVector = strBlock.asVector();
          if (strVector == null) {
            return eval(page.getPositionCount(), strBlock, regexBlock, newStrBlock);
          }
          BytesRefVector regexVector = regexBlock.asVector();
          if (regexVector == null) {
            return eval(page.getPositionCount(), strBlock, regexBlock, newStrBlock);
          }
          BytesRefVector newStrVector = newStrBlock.asVector();
          if (newStrVector == null) {
            return eval(page.getPositionCount(), strBlock, regexBlock, newStrBlock);
          }
          return eval(page.getPositionCount(), strVector, regexVector, newStrVector);
        }
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock regexBlock,
      BytesRefBlock newStrBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef regexScratch = new BytesRef();
      BytesRef newStrScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (strBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (strBlock.getValueCount(p) != 1) {
          if (strBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (regexBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (regexBlock.getValueCount(p) != 1) {
          if (regexBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (newStrBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (newStrBlock.getValueCount(p) != 1) {
          if (newStrBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendBytesRef(Replace.process(strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), regexBlock.getBytesRef(regexBlock.getFirstValueIndex(p), regexScratch), newStrBlock.getBytesRef(newStrBlock.getFirstValueIndex(p), newStrScratch)));
        } catch (PatternSyntaxException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector strVector, BytesRefVector regexVector,
      BytesRefVector newStrVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef regexScratch = new BytesRef();
      BytesRef newStrScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBytesRef(Replace.process(strVector.getBytesRef(p, strScratch), regexVector.getBytesRef(p, regexScratch), newStrVector.getBytesRef(p, newStrScratch)));
        } catch (PatternSyntaxException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ReplaceEvaluator[" + "str=" + str + ", regex=" + regex + ", newStr=" + newStr + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, regex, newStr);
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

    private final EvalOperator.ExpressionEvaluator.Factory regex;

    private final EvalOperator.ExpressionEvaluator.Factory newStr;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory regex,
        EvalOperator.ExpressionEvaluator.Factory newStr) {
      this.source = source;
      this.str = str;
      this.regex = regex;
      this.newStr = newStr;
    }

    @Override
    public ReplaceEvaluator get(DriverContext context) {
      return new ReplaceEvaluator(source, str.get(context), regex.get(context), newStr.get(context), context);
    }

    @Override
    public String toString() {
      return "ReplaceEvaluator[" + "str=" + str + ", regex=" + regex + ", newStr=" + newStr + "]";
    }
  }
}
