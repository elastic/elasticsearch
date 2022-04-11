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

public class UnpairedTTestState implements TTestState {

    public static final String NAME = "U";

    private final TTestStats a;
    private final TTestStats b;
    private boolean homoscedastic;
    private int tails;

    public UnpairedTTestState(TTestStats a, TTestStats b, boolean homoscedastic, int tails) {
        this.a = a;
        this.b = b;
        this.homoscedastic = homoscedastic;
        this.tails = tails;
    }

    public UnpairedTTestState(StreamInput in) throws IOException {
        a = new TTestStats(in);
        b = new TTestStats(in);
        homoscedastic = in.readBoolean();
        tails = in.readVInt();
    }

    @Override
    public double getValue() {
        if (a.count < 2 || b.count < 2) {
            return Double.NaN;
        }

        if (homoscedastic) {
            long n = a.count + b.count - 2;
            double variance = ((a.count - 1) * a.variance() + (b.count - 1) * b.variance()) / n;
            double nn = (1.0 / a.count + 1.0 / b.count);
            return p(variance * nn, n);
        } else {
            double s2an = a.variance() / a.count;
            double s2bn = b.variance() / b.count;
            double variance = s2an + s2bn;
            double degreeOfFreedom = variance * variance / (s2an * s2an / (a.count - 1) + s2bn * s2bn / (b.count - 1));
            return p(variance, degreeOfFreedom);
        }
    }

    private double p(double sd2, double degreesOfFreedom) {
        if (degreesOfFreedom < 0) {
            return Double.NaN;
        }
        double sd = Math.sqrt(sd2);
        double meanDiff = a.average() - b.average();
        double t = Math.abs(meanDiff / sd);
        TDistribution dist = new TDistribution(degreesOfFreedom);
        return dist.cumulativeProbability(-t) * tails;
    }

    @Override
    public TTestState reduce(Stream<TTestState> states) {
        TTestStats.Reducer reducerA = new TTestStats.Reducer();
        TTestStats.Reducer reducerB = new TTestStats.Reducer();
        states.forEach(tTestState -> {
            UnpairedTTestState state = (UnpairedTTestState) tTestState;
            if (state.homoscedastic != homoscedastic) {
                throw new IllegalStateException(
                    "Incompatible homoscedastic mode in the reduce. Expected " + state.homoscedastic + " reduced with " + homoscedastic
                );
            }
            if (state.tails != tails) {
                throw new IllegalStateException(
                    "Incompatible tails value in the reduce. Expected " + state.tails + " reduced with " + tails
                );
            }
            reducerA.accept(state.a);
            reducerB.accept(state.b);
        });
        return new UnpairedTTestState(reducerA.result(), reducerB.result(), homoscedastic, tails);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        a.writeTo(out);
        b.writeTo(out);
        out.writeBoolean(homoscedastic);
        out.writeVInt(tails);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnpairedTTestState that = (UnpairedTTestState) o;
        return homoscedastic == that.homoscedastic && tails == that.tails && a.equals(that.a) && b.equals(that.b);
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b, homoscedastic, tails);
    }
}
