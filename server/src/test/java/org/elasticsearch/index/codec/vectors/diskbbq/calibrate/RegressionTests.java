/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public License
 * v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;

public class RegressionTests extends ESTestCase {

    public void testFitOlsRecoversKnownLine() {
        double[] x = { 0.0, 1.0, 2.0, 3.0, 4.0 };
        double[] y = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            y[i] = 2.0 + 3.0 * x[i];
        }
        Regression.OLSResult res = Regression.fitOls(x, y);
        assertThat(res.beta0(), closeTo(2.0, 1e-10));
        assertThat(res.beta1(), closeTo(3.0, 1e-10));
        assertThat(Regression.rSquared(x, y, res), closeTo(1.0, 1e-10));
    }

    public void testFitOlsReturnsZeroForTooFewPoints() {
        Regression.OLSResult res = Regression.fitOls(new double[] { 1.0, 2.0 }, new double[] { 3.0, 4.0 });
        assertSame(Regression.OLSResult.ZERO, res);
    }

    public void testPredictOlsUsesFittedCoefficients() {
        double[] x = { 1.0, 2.0, 3.0, 4.0, 5.0 };
        double[] y = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            y[i] = 1.0 + 0.5 * x[i];
        }
        Regression.OLSResult res = Regression.fitOls(x, y);
        Regression.Prediction p = Regression.predictOls(res, 10.0);
        assertThat(p.mean(), closeTo(6.0, 1e-9));
        assertTrue(Double.isFinite(p.std()));
    }

    public void testOlsAccumulatorMatchesBatchFitOls() {
        double[] x = { 0.0, 1.0, 2.0, 3.0, 4.0 };
        double[] y = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            y[i] = 2.0 + 3.0 * x[i];
        }
        Regression.OLSAccumulator acc = new Regression.OLSAccumulator();
        acc.update(x, y);
        Regression.OLSResult batch = Regression.fitOls(x, y);
        Regression.OLSResult incremental = acc.fit();
        assertThat(incremental.beta0(), closeTo(batch.beta0(), 1e-10));
        assertThat(incremental.beta1(), closeTo(batch.beta1(), 1e-10));
    }

    public void testOlsAccumulatorFitPluginReusesSlope() {
        double[] x = { 1.0, 2.0, 3.0, 4.0, 5.0 };
        double[] y = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            y[i] = 1.0 + 0.5 * x[i];
        }
        Regression.OLSResult scaling = Regression.fitOls(x, y);
        Regression.OLSAccumulator acc = new Regression.OLSAccumulator();
        acc.update(x, y);
        Regression.OLSResult plugin = acc.fitPlugin(scaling);
        assertThat(plugin.beta1(), closeTo(scaling.beta1(), 1e-10));
        assertThat(plugin.beta0(), closeTo(scaling.beta0(), 1e-9));
    }
}
