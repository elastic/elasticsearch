// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link CIDRMatch}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class CIDRMatchEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CIDRMatchEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator ip;

  private final EvalOperator.ExpressionEvaluator[] cidrs;

  private final DriverContext driverContext;

  private Warnings warnings;

  public CIDRMatchEvaluator(Source source, EvalOperator.ExpressionEvaluator ip,
      EvalOperator.ExpressionEvaluator[] cidrs, DriverContext driverContext) {
    this.source = source;
    this.ip = ip;
    this.cidrs = cidrs;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock ipBlock = (BytesRefBlock) ip.eval(page)) {
      BytesRefBlock[] cidrsBlocks = new BytesRefBlock[cidrs.length];
      try (Releasable cidrsRelease = Releasables.wrap(cidrsBlocks)) {
        for (int i = 0; i < cidrsBlocks.length; i++) {
          cidrsBlocks[i] = (BytesRefBlock)cidrs[i].eval(page);
        }
        BytesRefVector ipVector = ipBlock.asVector();
        if (ipVector == null) {
          return eval(page.getPositionCount(), ipBlock, cidrsBlocks);
        }
        BytesRefVector[] cidrsVectors = new BytesRefVector[cidrs.length];
        for (int i = 0; i < cidrsBlocks.length; i++) {
          cidrsVectors[i] = cidrsBlocks[i].asVector();
          if (cidrsVectors[i] == null) {
            return eval(page.getPositionCount(), ipBlock, cidrsBlocks);
          }
        }
        return eval(page.getPositionCount(), ipVector, cidrsVectors).asBlock();
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += ip.baseRamBytesUsed();
    for (EvalOperator.ExpressionEvaluator e : cidrs) {
      baseRamBytesUsed += e.baseRamBytesUsed();
    }
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock ipBlock, BytesRefBlock[] cidrsBlocks) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef ipScratch = new BytesRef();
      BytesRef[] cidrsValues = new BytesRef[cidrs.length];
      BytesRef[] cidrsScratch = new BytesRef[cidrs.length];
      for (int i = 0; i < cidrs.length; i++) {
        cidrsScratch[i] = new BytesRef();
      }
      position: for (int p = 0; p < positionCount; p++) {
        switch (ipBlock.getValueCount(p)) {
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
        for (int i = 0; i < cidrsBlocks.length; i++) {
          switch (cidrsBlocks[i].getValueCount(p)) {
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
        }
        BytesRef ip = ipBlock.getBytesRef(ipBlock.getFirstValueIndex(p), ipScratch);
        // unpack cidrsBlocks into cidrsValues
        for (int i = 0; i < cidrsBlocks.length; i++) {
          int o = cidrsBlocks[i].getFirstValueIndex(p);
          cidrsValues[i] = cidrsBlocks[i].getBytesRef(o, cidrsScratch[i]);
        }
        result.appendBoolean(CIDRMatch.process(ip, cidrsValues));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BytesRefVector ipVector,
      BytesRefVector[] cidrsVectors) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      BytesRef ipScratch = new BytesRef();
      BytesRef[] cidrsValues = new BytesRef[cidrs.length];
      BytesRef[] cidrsScratch = new BytesRef[cidrs.length];
      for (int i = 0; i < cidrs.length; i++) {
        cidrsScratch[i] = new BytesRef();
      }
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef ip = ipVector.getBytesRef(p, ipScratch);
        // unpack cidrsVectors into cidrsValues
        for (int i = 0; i < cidrsVectors.length; i++) {
          cidrsValues[i] = cidrsVectors[i].getBytesRef(p, cidrsScratch[i]);
        }
        result.appendBoolean(p, CIDRMatch.process(ip, cidrsValues));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "CIDRMatchEvaluator[" + "ip=" + ip + ", cidrs=" + Arrays.toString(cidrs) + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(ip, () -> Releasables.close(cidrs));
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

    private final EvalOperator.ExpressionEvaluator.Factory ip;

    private final EvalOperator.ExpressionEvaluator.Factory[] cidrs;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory ip,
        EvalOperator.ExpressionEvaluator.Factory[] cidrs) {
      this.source = source;
      this.ip = ip;
      this.cidrs = cidrs;
    }

    @Override
    public CIDRMatchEvaluator get(DriverContext context) {
      EvalOperator.ExpressionEvaluator[] cidrs = Arrays.stream(this.cidrs).map(a -> a.get(context)).toArray(EvalOperator.ExpressionEvaluator[]::new);
      return new CIDRMatchEvaluator(source, ip.get(context), cidrs, context);
    }

    @Override
    public String toString() {
      return "CIDRMatchEvaluator[" + "ip=" + ip + ", cidrs=" + Arrays.toString(cidrs) + "]";
    }
  }
}
