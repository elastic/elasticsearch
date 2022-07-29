/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.apache.commons.math3.distribution.TDistribution;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

public class PairedTTestState implements TTestState {

    public static final String NAME = "P";

    private final TTestStats stats;

    private final int tails;

    public PairedTTestState(TTestStats stats, int tails) {
        this.stats = stats;
        this.tails = tails;
    }

    public PairedTTestState(StreamInput in) throws IOException {
        stats = new TTestStats(in);
        tails = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        stats.writeTo(out);
        out.writeVInt(tails);
    }

    @Override
    public double getValue() {
        if (stats.count < 2) {
            return Double.NaN;
        }
        long n = stats.count - 1;
        double meanDiff = stats.sum / stats.count;
        double variance = (stats.sumOfSqrs - ((stats.sum * stats.sum) / stats.count)) / stats.count;
        if (variance <= 0.0) {
            return meanDiff == 0.0 ? Double.NaN : 0.0;
        }
        double stdDiv = Math.sqrt(variance);
        double stdErr = stdDiv / Math.sqrt(n);
        double t = Math.abs(meanDiff / stdErr);
        TDistribution dist = new TDistribution(n);
        return dist.cumulativeProbability(-t) * tails;
    }

    @Override
    public TTestState reduce(Stream<TTestState> states) {
        TTestStats.Reducer reducer = new TTestStats.Reducer();
        states.forEach(tTestState -> {
            PairedTTestState state = (PairedTTestState) tTestState;
            reducer.accept(state.stats);
            if (state.tails != tails) {
                throw new IllegalStateException(
                    "Incompatible tails value in the reduce. Expected " + state.tails + " reduced with " + tails
                );
            }
        });
        return new PairedTTestState(reducer.result(), tails);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairedTTestState that = (PairedTTestState) o;
        return tails == that.tails && stats.equals(that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats, tails);
    }
}
