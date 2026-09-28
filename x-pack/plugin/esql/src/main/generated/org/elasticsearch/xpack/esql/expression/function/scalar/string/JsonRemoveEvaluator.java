// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.Set;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link JsonRemove}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class JsonRemoveEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(JsonRemoveEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator object;

  private final Set<String> fields;

  private final DriverContext driverContext;

  private Warnings warnings;

  public JsonRemoveEvaluator(Source source, ExpressionEvaluator object, Set<String> fields,
      DriverContext driverContext) {
    this.source = source;
    this.object = object;
    this.fields = fields;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock objectBlock = (BytesRefBlock) object.eval(page)) {
      BytesRefVector objectVector = objectBlock.asVector();
      if (objectVector == null) {
        return eval(page.getPositionCount(), objectBlock);
      }
      return eval(page.getPositionCount(), objectVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += object.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock objectBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef objectScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (objectBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (objectBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        BytesRef object = objectBlock.getBytesRef(objectBlock.getFirstValueIndex(p), objectScratch);
        try {
          result.appendBytesRef(JsonRemove.process(object, this.fields));
        } catch (IOException | IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector objectVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef objectScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef object = objectVector.getBytesRef(p, objectScratch);
        try {
          result.appendBytesRef(JsonRemove.process(object, this.fields));
        } catch (IOException | IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "JsonRemoveEvaluator[" + "object=" + object + ", fields=" + fields + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(object);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory object;

    private final Set<String> fields;

    public Factory(Source source, ExpressionEvaluator.Factory object, Set<String> fields) {
      this.source = source;
      this.object = object;
      this.fields = fields;
    }

    @Override
    public JsonRemoveEvaluator get(DriverContext context) {
      return new JsonRemoveEvaluator(source, object.get(context), fields, context);
    }

    @Override
    public String toString() {
      return "JsonRemoveEvaluator[" + "object=" + object + ", fields=" + fields + "]";
    }
  }
}
