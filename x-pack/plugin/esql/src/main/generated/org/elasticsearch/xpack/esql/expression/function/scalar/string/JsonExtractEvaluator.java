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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link JsonExtract}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class JsonExtractEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(JsonExtractEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator jsonInput;

  private final EvalOperator.ExpressionEvaluator path;

  private final DriverContext driverContext;

  private Warnings warnings;

  public JsonExtractEvaluator(Source source, EvalOperator.ExpressionEvaluator jsonInput,
      EvalOperator.ExpressionEvaluator path, DriverContext driverContext) {
    this.source = source;
    this.jsonInput = jsonInput;
    this.path = path;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock jsonInputBlock = (BytesRefBlock) jsonInput.eval(page)) {
      try (BytesRefBlock pathBlock = (BytesRefBlock) path.eval(page)) {
        BytesRefVector jsonInputVector = jsonInputBlock.asVector();
        if (jsonInputVector == null) {
          return eval(page.getPositionCount(), jsonInputBlock, pathBlock);
        }
        BytesRefVector pathVector = pathBlock.asVector();
        if (pathVector == null) {
          return eval(page.getPositionCount(), jsonInputBlock, pathBlock);
        }
        return eval(page.getPositionCount(), jsonInputVector, pathVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += jsonInput.baseRamBytesUsed();
    baseRamBytesUsed += path.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock jsonInputBlock,
      BytesRefBlock pathBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef jsonInputScratch = new BytesRef();
      BytesRef pathScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (jsonInputBlock.getValueCount(p)) {
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
        switch (pathBlock.getValueCount(p)) {
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
        BytesRef jsonInput = jsonInputBlock.getBytesRef(jsonInputBlock.getFirstValueIndex(p), jsonInputScratch);
        BytesRef path = pathBlock.getBytesRef(pathBlock.getFirstValueIndex(p), pathScratch);
        try {
          JsonExtract.process(result, jsonInput, path);
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector jsonInputVector,
      BytesRefVector pathVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef jsonInputScratch = new BytesRef();
      BytesRef pathScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef jsonInput = jsonInputVector.getBytesRef(p, jsonInputScratch);
        BytesRef path = pathVector.getBytesRef(p, pathScratch);
        try {
          JsonExtract.process(result, jsonInput, path);
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
    return "JsonExtractEvaluator[" + "jsonInput=" + jsonInput + ", path=" + path + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(jsonInput, path);
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

    private final EvalOperator.ExpressionEvaluator.Factory jsonInput;

    private final EvalOperator.ExpressionEvaluator.Factory path;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory jsonInput,
        EvalOperator.ExpressionEvaluator.Factory path) {
      this.source = source;
      this.jsonInput = jsonInput;
      this.path = path;
    }

    @Override
    public JsonExtractEvaluator get(DriverContext context) {
      return new JsonExtractEvaluator(source, jsonInput.get(context), path.get(context), context);
    }

    @Override
    public String toString() {
      return "JsonExtractEvaluator[" + "jsonInput=" + jsonInput + ", path=" + path + "]";
    }
  }
}
