// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Class;
import java.lang.IllegalAccessException;
import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.InstantiationException;
import java.lang.Override;
import java.lang.String;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.JitConstantSpinner;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link JsonExtract}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public abstract class JsonExtractConstantEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(JsonExtractConstantEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator str;

  private final DriverContext driverContext;

  private Warnings warnings;

  public JsonExtractConstantEvaluator(Source source, ExpressionEvaluator str,
      DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.driverContext = driverContext;
  }

  protected abstract JsonPath path();

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      BytesRefVector strVector = strBlock.asVector();
      if (strVector == null) {
        return eval(page.getPositionCount(), strBlock);
      }
      return eval(page.getPositionCount(), strVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += str.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
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
        BytesRef str = strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch);
        try {
          JsonExtract.processConstant(result, str, path());
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector strVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef str = strVector.getBytesRef(p, strScratch);
        try {
          JsonExtract.processConstant(result, str, path());
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
    return "JsonExtractConstantEvaluator[" + "str=" + str + ", path=" + path() + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory str;

    private final JsonPath path;

    public Factory(Source source, ExpressionEvaluator.Factory str, JsonPath path) {
      this.source = source;
      this.str = str;
      this.path = path;
    }

    @Override
    public JsonExtractConstantEvaluator get(DriverContext context) {
      Optional<Class<? extends JsonExtractConstantEvaluator>> spunClassOpt = JitConstantSpinner.referenceConstantSubclass(JsonExtractConstantEvaluator.class, "path", JsonPath.class, this.path);
      if (spunClassOpt.isPresent()) {
        Class<? extends JsonExtractConstantEvaluator> spunClass = spunClassOpt.get();
        try {
          return (JsonExtractConstantEvaluator) spunClass.getConstructors()[0].newInstance(source, str.get(context), context);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          throw new IllegalStateException("failed to construct JIT-spun evaluator for JsonExtractConstantEvaluator", e);
        }
      }
      return new Fallback(source, str.get(context), this.path, context);
    }

    @Override
    public String toString() {
      return "JsonExtractConstantEvaluator[" + "str=" + str + ", path=" + path + "]";
    }
  }

  /**
   * Concrete fallback used when {@link JitConstantSpinner} returns {@code Optional.empty()}
   * (admission filter rejected the spin). The constant lives in a regular
   * instance field — no JIT-time constant folding, but the per-row work
   * runs correctly. The Factory chooses between this and the spun subclass.
   */
  public static final class Fallback extends JsonExtractConstantEvaluator {
    private final JsonPath path;

    public Fallback(Source source, ExpressionEvaluator str, JsonPath path,
        DriverContext driverContext) {
      super(source, str, driverContext);
      this.path = path;
    }

    @Override
    protected final JsonPath path() {
      return path;
    }
  }
}
