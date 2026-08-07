// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link CIDRMatch}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class CIDRMatchEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CIDRMatchEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator ip;

  private final ExpressionEvaluator cidr;

  private final BytesRef ipScratch;

  private final BytesRef cidrScratch;

  private final DriverContext driverContext;

  private Warnings warnings;

  public CIDRMatchEvaluator(Source source, ExpressionEvaluator ip, ExpressionEvaluator cidr,
      BytesRef ipScratch, BytesRef cidrScratch, DriverContext driverContext) {
    this.source = source;
    this.ip = ip;
    this.cidr = cidr;
    this.ipScratch = ipScratch;
    this.cidrScratch = cidrScratch;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock ipBlock = (BytesRefBlock) ip.eval(page)) {
      try (BytesRefBlock cidrBlock = (BytesRefBlock) cidr.eval(page)) {
        return eval(page.getPositionCount(), ipBlock, cidrBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += ip.baseRamBytesUsed();
    baseRamBytesUsed += cidr.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock ipBlock, BytesRefBlock cidrBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(CIDRMatch.process(p, ipBlock, cidrBlock, this.ipScratch, this.cidrScratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "CIDRMatchEvaluator[" + "ip=" + ip + ", cidr=" + cidr + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(ip, cidr);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory ip;

    private final ExpressionEvaluator.Factory cidr;

    private final Function<DriverContext, BytesRef> ipScratch;

    private final Function<DriverContext, BytesRef> cidrScratch;

    public Factory(Source source, ExpressionEvaluator.Factory ip, ExpressionEvaluator.Factory cidr,
        Function<DriverContext, BytesRef> ipScratch,
        Function<DriverContext, BytesRef> cidrScratch) {
      this.source = source;
      this.ip = ip;
      this.cidr = cidr;
      this.ipScratch = ipScratch;
      this.cidrScratch = cidrScratch;
    }

    @Override
    public CIDRMatchEvaluator get(DriverContext context) {
      return new CIDRMatchEvaluator(source, ip.get(context), cidr.get(context), ipScratch.apply(context), cidrScratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "CIDRMatchEvaluator[" + "ip=" + ip + ", cidr=" + cidr + "]";
    }
  }
}
