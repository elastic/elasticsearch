// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Override;
import java.lang.String;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Replace}.
 * This class is generated. Do not edit it.
 */
public final class ReplaceConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator str;

  private final Pattern regex;

  private final EvalOperator.ExpressionEvaluator newStr;

  private final DriverContext driverContext;

  public ReplaceConstantEvaluator(Source source, EvalOperator.ExpressionEvaluator str,
      Pattern regex, EvalOperator.ExpressionEvaluator newStr, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.str = str;
    this.regex = regex;
    this.newStr = newStr;
    this.driverContext = driverContext;
  }

  @Override
  public Block.Ref eval(Page page) {
    try (Block.Ref strRef = str.eval(page)) {
      if (strRef.block().areAllValuesNull()) {
        return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
      }
      BytesRefBlock strBlock = (BytesRefBlock) strRef.block();
      try (Block.Ref newStrRef = newStr.eval(page)) {
        if (newStrRef.block().areAllValuesNull()) {
          return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
        }
        BytesRefBlock newStrBlock = (BytesRefBlock) newStrRef.block();
        BytesRefVector strVector = strBlock.asVector();
        if (strVector == null) {
          return Block.Ref.floating(eval(page.getPositionCount(), strBlock, newStrBlock));
        }
        BytesRefVector newStrVector = newStrBlock.asVector();
        if (newStrVector == null) {
          return Block.Ref.floating(eval(page.getPositionCount(), strBlock, newStrBlock));
        }
        return Block.Ref.floating(eval(page.getPositionCount(), strVector, newStrVector));
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock newStrBlock) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    BytesRef newStrScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (strBlock.isNull(p) || strBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (newStrBlock.isNull(p) || newStrBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      try {
        result.appendBytesRef(Replace.process(strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), regex, newStrBlock.getBytesRef(newStrBlock.getFirstValueIndex(p), newStrScratch)));
      } catch (PatternSyntaxException e) {
        warnings.registerException(e);
        result.appendNull();
      }
    }
    return result.build();
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector strVector,
      BytesRefVector newStrVector) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    BytesRef newStrScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      try {
        result.appendBytesRef(Replace.process(strVector.getBytesRef(p, strScratch), regex, newStrVector.getBytesRef(p, newStrScratch)));
      } catch (PatternSyntaxException e) {
        warnings.registerException(e);
        result.appendNull();
      }
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "ReplaceConstantEvaluator[" + "str=" + str + ", regex=" + regex + ", newStr=" + newStr + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, newStr);
  }
}
