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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.ConstantMethodResultSpecializer;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link EndsWith}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public abstract class EndsWithConstantEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(EndsWithConstantEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator str;

  private final DriverContext driverContext;

  private Warnings warnings;

  public EndsWithConstantEvaluator(Source source, ExpressionEvaluator str,
      DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.driverContext = driverContext;
  }

  protected abstract BytesRef suffix();

  protected String pathLabel() {
    return "jit-folded";
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      BytesRefVector strVector = strBlock.asVector();
      if (strVector == null) {
        return eval(page.getPositionCount(), strBlock);
      }
      return eval(page.getPositionCount(), strVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += str.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock strBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
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
        result.appendBoolean(EndsWith.processConstant(str, suffix()));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BytesRefVector strVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef str = strVector.getBytesRef(p, strScratch);
        result.appendBoolean(p, EndsWith.processConstant(str, suffix()));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "EndsWithConstantEvaluator[" + "str=" + str + ", suffix=" + suffix() + "]" + " (" + pathLabel() + ")";
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

    private final BytesRef suffix;

    public Factory(Source source, ExpressionEvaluator.Factory str, BytesRef suffix) {
      this.source = source;
      this.str = str;
      this.suffix = suffix;
    }

    @Override
    public EndsWithConstantEvaluator get(DriverContext context) {
      Optional<Class<? extends EndsWithConstantEvaluator>> constantSpecializedClassOpt = ConstantMethodResultSpecializer.SHARED.specializeReference(EndsWithConstantEvaluator.class, "suffix", BytesRef.class, this.suffix);
      if (constantSpecializedClassOpt.isPresent()) {
        Class<? extends EndsWithConstantEvaluator> constantSpecializedClass = constantSpecializedClassOpt.get();
        try {
          return (EndsWithConstantEvaluator) constantSpecializedClass.getConstructors()[0].newInstance(source, str.get(context), context);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          throw new IllegalStateException("failed to construct specialized evaluator for EndsWithConstantEvaluator", e);
        }
      }
      return new Standard(source, str.get(context), this.suffix, context);
    }

    @Override
    public String toString() {
      return "EndsWithConstantEvaluator[" + "str=" + str + ", suffix=" + suffix + "]";
    }
  }

  /**
   * Concrete non-constant-specialized subclass used when {@link ConstantMethodResultSpecializer} returns {@code Optional.empty()}
   * (admission filter rejected the spin). The constant lives in a regular
   * instance field — no JIT-time constant folding, but the per-row work
   * runs correctly. The Factory chooses between this and the constant-specialized subclass.
   */
  public static final class Standard extends EndsWithConstantEvaluator {
    private final BytesRef suffix;

    public Standard(Source source, ExpressionEvaluator str, BytesRef suffix,
        DriverContext driverContext) {
      super(source, str, driverContext);
      this.suffix = suffix;
    }

    @Override
    protected final BytesRef suffix() {
      return suffix;
    }

    @Override
    protected final String pathLabel() {
      return "standard";
    }
  }
}
