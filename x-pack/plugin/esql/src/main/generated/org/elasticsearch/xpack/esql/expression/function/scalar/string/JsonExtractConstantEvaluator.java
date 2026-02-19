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
public final class JsonExtractConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(JsonExtractConstantEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator jsonInput;

  private final JsonExtract.ParsedPath path;

  private final DriverContext driverContext;

  private Warnings warnings;

  public JsonExtractConstantEvaluator(Source source, EvalOperator.ExpressionEvaluator jsonInput,
      JsonExtract.ParsedPath path, DriverContext driverContext) {
    this.source = source;
    this.jsonInput = jsonInput;
    this.path = path;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock jsonInputBlock = (BytesRefBlock) jsonInput.eval(page)) {
      BytesRefVector jsonInputVector = jsonInputBlock.asVector();
      if (jsonInputVector == null) {
        return eval(page.getPositionCount(), jsonInputBlock);
      }
      return eval(page.getPositionCount(), jsonInputVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += jsonInput.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock jsonInputBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef jsonInputScratch = new BytesRef();
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
        BytesRef jsonInput = jsonInputBlock.getBytesRef(jsonInputBlock.getFirstValueIndex(p), jsonInputScratch);
        try {
          JsonExtract.processConstant(result, jsonInput, this.path);
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector jsonInputVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef jsonInputScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef jsonInput = jsonInputVector.getBytesRef(p, jsonInputScratch);
        try {
          JsonExtract.processConstant(result, jsonInput, this.path);
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
    return "JsonExtractConstantEvaluator[" + "jsonInput=" + jsonInput + ", path=" + path + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(jsonInput);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory jsonInput;

    private final JsonExtract.ParsedPath path;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory jsonInput,
        JsonExtract.ParsedPath path) {
      this.source = source;
      this.jsonInput = jsonInput;
      this.path = path;
    }

    @Override
    public JsonExtractConstantEvaluator get(DriverContext context) {
      return new JsonExtractConstantEvaluator(source, jsonInput.get(context), path, context);
    }

    @Override
    public String toString() {
      return "JsonExtractConstantEvaluator[" + "jsonInput=" + jsonInput + ", path=" + path + "]";
    }
  }
}
