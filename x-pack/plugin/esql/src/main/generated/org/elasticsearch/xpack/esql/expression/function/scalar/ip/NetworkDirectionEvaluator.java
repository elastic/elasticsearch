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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link NetworkDirection}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class NetworkDirectionEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(NetworkDirectionEvaluator.class);

  private final Source source;

  private final BytesRef scratch;

  private final BytesRef netScratch;

  private final ExpressionEvaluator sourceIp;

  private final ExpressionEvaluator destinationIp;

  private final ExpressionEvaluator networks;

  private final DriverContext driverContext;

  private Warnings warnings;

  public NetworkDirectionEvaluator(Source source, BytesRef scratch, BytesRef netScratch,
      ExpressionEvaluator sourceIp, ExpressionEvaluator destinationIp, ExpressionEvaluator networks,
      DriverContext driverContext) {
    this.source = source;
    this.scratch = scratch;
    this.netScratch = netScratch;
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
          return eval(page.getPositionCount(), sourceIpBlock, destinationIpBlock, networksBlock);
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
        boolean allBlocksAreNulls = true;
        switch (sourceIpBlock.getValueCount(p)) {
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
        switch (destinationIpBlock.getValueCount(p)) {
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
        if (!networksBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        BytesRef sourceIp = sourceIpBlock.getBytesRef(sourceIpBlock.getFirstValueIndex(p), sourceIpScratch);
        BytesRef destinationIp = destinationIpBlock.getBytesRef(destinationIpBlock.getFirstValueIndex(p), destinationIpScratch);
        try {
          NetworkDirection.process(result, this.scratch, this.netScratch, sourceIp, destinationIp, p, networksBlock);
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
    return "NetworkDirectionEvaluator[" + "sourceIp=" + sourceIp + ", destinationIp=" + destinationIp + ", networks=" + networks + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(sourceIp, destinationIp, networks);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final Function<DriverContext, BytesRef> scratch;

    private final Function<DriverContext, BytesRef> netScratch;

    private final ExpressionEvaluator.Factory sourceIp;

    private final ExpressionEvaluator.Factory destinationIp;

    private final ExpressionEvaluator.Factory networks;

    public Factory(Source source, Function<DriverContext, BytesRef> scratch,
        Function<DriverContext, BytesRef> netScratch, ExpressionEvaluator.Factory sourceIp,
        ExpressionEvaluator.Factory destinationIp, ExpressionEvaluator.Factory networks) {
      this.source = source;
      this.scratch = scratch;
      this.netScratch = netScratch;
      this.sourceIp = sourceIp;
      this.destinationIp = destinationIp;
      this.networks = networks;
    }

    @Override
    public NetworkDirectionEvaluator get(DriverContext context) {
      return new NetworkDirectionEvaluator(source, scratch.apply(context), netScratch.apply(context), sourceIp.get(context), destinationIp.get(context), networks.get(context), context);
    }

    @Override
    public String toString() {
      return "NetworkDirectionEvaluator[" + "sourceIp=" + sourceIp + ", destinationIp=" + destinationIp + ", networks=" + networks + "]";
    }
  }
}
