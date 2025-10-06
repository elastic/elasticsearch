// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link NetworkDirection}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class NetworkDirectionEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(NetworkDirectionEvaluator.class);

  private final Source source;

  private final BytesRef scratch;

  private final EvalOperator.ExpressionEvaluator sourceIp;

  private final EvalOperator.ExpressionEvaluator destinationIp;

  private final EvalOperator.ExpressionEvaluator networks;

  private final DriverContext driverContext;

  private Warnings warnings;

  public NetworkDirectionEvaluator(Source source, BytesRef scratch,
      EvalOperator.ExpressionEvaluator sourceIp, EvalOperator.ExpressionEvaluator destinationIp,
      EvalOperator.ExpressionEvaluator networks, DriverContext driverContext) {
    this.source = source;
    this.scratch = scratch;
    this.sourceIp = sourceIp;
    this.destinationIp = destinationIp;
    this.networks = networks;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock sourceIpBlock = (BytesRefBlock) sourceIp.eval(page)) {
      try (BytesRefBlock destinationIpBlock = (BytesRefBlock) destinationIp.eval(page)) {
        try (BytesRefBlock networksBlock = (BytesRefBlock) networks.eval(page)) {
          BytesRefVector sourceIpVector = sourceIpBlock.asVector();
          if (sourceIpVector == null) {
            return eval(page.getPositionCount(), sourceIpBlock, destinationIpBlock, networksBlock);
          }
          BytesRefVector destinationIpVector = destinationIpBlock.asVector();
          if (destinationIpVector == null) {
            return eval(page.getPositionCount(), sourceIpBlock, destinationIpBlock, networksBlock);
          }
          return eval(page.getPositionCount(), sourceIpVector, destinationIpVector, networksBlock).asBlock();
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += sourceIp.baseRamBytesUsed();
    baseRamBytesUsed += destinationIp.baseRamBytesUsed();
    baseRamBytesUsed += networks.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock sourceIpBlock,
      BytesRefBlock destinationIpBlock, BytesRefBlock networksBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef sourceIpScratch = new BytesRef();
      BytesRef destinationIpScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (sourceIpBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (sourceIpBlock.getValueCount(p) != 1) {
          if (sourceIpBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (destinationIpBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (destinationIpBlock.getValueCount(p) != 1) {
          if (destinationIpBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (networksBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (networksBlock.getValueCount(p) != 1) {
          if (networksBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        BytesRef sourceIp = sourceIpBlock.getBytesRef(sourceIpBlock.getFirstValueIndex(p), sourceIpScratch);
        BytesRef destinationIp = destinationIpBlock.getBytesRef(destinationIpBlock.getFirstValueIndex(p), destinationIpScratch);
        result.appendBytesRef(NetworkDirection.process(this.scratch, sourceIp, destinationIp, p, networksBlock));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector sourceIpVector,
      BytesRefVector destinationIpVector, BytesRefBlock networksBlock) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef sourceIpScratch = new BytesRef();
      BytesRef destinationIpScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef sourceIp = sourceIpVector.getBytesRef(p, sourceIpScratch);
        BytesRef destinationIp = destinationIpVector.getBytesRef(p, destinationIpScratch);
        result.appendBytesRef(NetworkDirection.process(this.scratch, sourceIp, destinationIp, p, networksBlock));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "NetworkDirectionEvaluator[" + "sourceIp=" + sourceIp + ", destinationIp=" + destinationIp + ", networks=" + networks + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(sourceIp, destinationIp, networks);
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

    private final Function<DriverContext, BytesRef> scratch;

    private final EvalOperator.ExpressionEvaluator.Factory sourceIp;

    private final EvalOperator.ExpressionEvaluator.Factory destinationIp;

    private final EvalOperator.ExpressionEvaluator.Factory networks;

    public Factory(Source source, Function<DriverContext, BytesRef> scratch,
        EvalOperator.ExpressionEvaluator.Factory sourceIp,
        EvalOperator.ExpressionEvaluator.Factory destinationIp,
        EvalOperator.ExpressionEvaluator.Factory networks) {
      this.source = source;
      this.scratch = scratch;
      this.sourceIp = sourceIp;
      this.destinationIp = destinationIp;
      this.networks = networks;
    }

    @Override
    public NetworkDirectionEvaluator get(DriverContext context) {
      return new NetworkDirectionEvaluator(source, scratch.apply(context), sourceIp.get(context), destinationIp.get(context), networks.get(context), context);
    }

    @Override
    public String toString() {
      return "NetworkDirectionEvaluator[" + "sourceIp=" + sourceIp + ", destinationIp=" + destinationIp + ", networks=" + networks + "]";
    }
  }
}
