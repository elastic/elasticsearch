/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.stream.DoubleStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class KDETests extends ESTestCase {

    public void testEmpty() {
        KDE kde = new KDE(new double[0], 1.06);
        assertThat(kde.size(), equalTo(0));
        assertThat(kde.data(), equalTo(new double[0]));
    }

    public void testCdfAndSf() {

        double[] data = DoubleStream.generate(() -> randomDoubleBetween(0.0, 100.0, true)).limit(101).toArray();
        Arrays.sort(data);
        KDE kde = new KDE(data, 1.06);

        // Very large and small limits are handled correctly.
        assertThat(kde.cdf(-1000.0).value(), closeTo(0.0, 1e-10));
        assertThat(kde.cdf(1000.0).value(), closeTo(1.0, 1e-10));
        assertThat(kde.sf(1000.0).value(), closeTo(0.0, 1e-10));
        assertThat(kde.sf(-1000.0).value(), closeTo(1.0, 1e-10));

        // Check the cdf and survival function are approximately equal to 0.5 for the median.
        {
            double median = kde.data()[kde.size() / 2];
            KDE.ValueAndMagnitude cdf = kde.cdf(median);
            KDE.ValueAndMagnitude sf = kde.sf(median);
            assertThat(cdf.value(), closeTo(0.5, 0.1));
            assertThat(sf.value(), closeTo(0.5, 0.1));
        }

        // Should approximately sum to 1.0 for some random data.
        for (int i = 0; i < 100; i++) {
            double x = randomDoubleBetween(-10.0, 110.0, true);
            KDE.ValueAndMagnitude cdf = kde.cdf(x);
            KDE.ValueAndMagnitude sf = kde.sf(x);
            assertThat(cdf.value() + sf.value(), closeTo(1.0, 1e-4));
        }
    }

    public void testSignificanceForUnderflow() {

        KDE kde = new KDE(new double[] { 1.0, 2.0, 3.0, 4.0, 5.0 }, 1.06);

        KDE.ValueAndMagnitude cdf = kde.cdf(-1000.0);
        KDE.ValueAndMagnitude sf = kde.sf(1000.0);

        assertThat(cdf.value(), equalTo(0.0));
        assertThat(sf.value(), equalTo(0.0));

        // Difference from data is larger for cdf than the survival function.
        assertThat(cdf.isMoreSignificant(sf), equalTo(true));
    }
}
