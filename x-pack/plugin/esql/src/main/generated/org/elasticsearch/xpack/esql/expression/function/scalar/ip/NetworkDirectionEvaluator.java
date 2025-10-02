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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
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

  private final EvalOperator.ExpressionEvaluator sourceIp;

  private final EvalOperator.ExpressionEvaluator destinationIp;

  private final EvalOperator.ExpressionEvaluator internalNetworks;

  private final BreakingBytesRefBuilder scratch;

  private final DriverContext driverContext;

  private Warnings warnings;

  public NetworkDirectionEvaluator(Source source, EvalOperator.ExpressionEvaluator sourceIp,
      EvalOperator.ExpressionEvaluator destinationIp,
      EvalOperator.ExpressionEvaluator internalNetworks, BreakingBytesRefBuilder scratch,
      DriverContext driverContext) {
    this.source = source;
    this.sourceIp = sourceIp;
    this.destinationIp = destinationIp;
    this.internalNetworks = internalNetworks;
    this.scratch = scratch;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock sourceIpBlock = (BytesRefBlock) sourceIp.eval(page)) {
      try (BytesRefBlock destinationIpBlock = (BytesRefBlock) destinationIp.eval(page)) {
        try (BytesRefBlock internalNetworksBlock = (BytesRefBlock) internalNetworks.eval(page)) {
          BytesRefVector sourceIpVector = sourceIpBlock.asVector();
          if (sourceIpVector == null) {
            return eval(page.getPositionCount(), sourceIpBlock, destinationIpBlock, internalNetworksBlock);
          }
          BytesRefVector destinationIpVector = destinationIpBlock.asVector();
          if (destinationIpVector == null) {
            return eval(page.getPositionCount(), sourceIpBlock, destinationIpBlock, internalNetworksBlock);
          }
          return eval(page.getPositionCount(), sourceIpVector, destinationIpVector, internalNetworksVector).asBlock();
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += sourceIp.baseRamBytesUsed();
    baseRamBytesUsed += destinationIp.baseRamBytesUsed();
    baseRamBytesUsed += internalNetworks.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock sourceIpBlock,
      BytesRefBlock destinationIpBlock, BytesRefBlock internalNetworksBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
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
        if (internalNetworksBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (internalNetworksBlock.getValueCount(p) != 1) {
          if (internalNetworksBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        BytesRef sourceIp = sourceIpBlock.getBytesRef(sourceIpBlock.getFirstValueIndex(p), sourceIpScratch);
        BytesRef destinationIp = destinationIpBlock.getBytesRef(destinationIpBlock.getFirstValueIndex(p), destinationIpScratch);
        result.appendBoolean(NetworkDirection.process(sourceIp, destinationIp, p, internalNetworksBlock, this.scratch));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BytesRefVector sourceIpVector,
      BytesRefVector destinationIpVector, BytesRefBlock internalNetworksVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      BytesRef sourceIpScratch = new BytesRef();
      BytesRef destinationIpScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef sourceIp = sourceIpVector.getBytesRef(p, sourceIpScratch);
        BytesRef destinationIp = destinationIpVector.getBytesRef(p, destinationIpScratch);
        result.appendBoolean(p, NetworkDirection.process(sourceIp, destinationIp, p, internalNetworksVector, this.scratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "NetworkDirectionEvaluator[" + "sourceIp=" + sourceIp + ", destinationIp=" + destinationIp + ", internalNetworks=" + internalNetworks + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(sourceIp, destinationIp, internalNetworks, scratch);
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

    private final EvalOperator.ExpressionEvaluator.Factory sourceIp;

    private final EvalOperator.ExpressionEvaluator.Factory destinationIp;

    private final EvalOperator.ExpressionEvaluator.Factory internalNetworks;

    private final Function<DriverContext, BreakingBytesRefBuilder> scratch;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory sourceIp,
        EvalOperator.ExpressionEvaluator.Factory destinationIp,
        EvalOperator.ExpressionEvaluator.Factory internalNetworks,
        Function<DriverContext, BreakingBytesRefBuilder> scratch) {
      this.source = source;
      this.sourceIp = sourceIp;
      this.destinationIp = destinationIp;
      this.internalNetworks = internalNetworks;
      this.scratch = scratch;
    }

    @Override
    public NetworkDirectionEvaluator get(DriverContext context) {
      return new NetworkDirectionEvaluator(source, sourceIp.get(context), destinationIp.get(context), internalNetworks.get(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "NetworkDirectionEvaluator[" + "sourceIp=" + sourceIp + ", destinationIp=" + destinationIp + ", internalNetworks=" + internalNetworks + "]";
    }
  }
}
