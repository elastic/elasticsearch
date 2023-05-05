/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import org.elasticsearch.geometry.Line;

public class GeometrySimplifierFrechetTests extends GeometrySimplifierTests {
    private SimplificationErrorCalculator calculator = new SimplificationErrorCalculator.FrechetErrorCalculator();

    @Override
    protected SimplificationErrorCalculator calculator() {
        return calculator;
    }

    protected void assertLineWithNarrowSpikes(Line simplified, int spikeCount) {
        // The frechet distance can save all spikes
        assertLineSpikes("Frechet", simplified, spikeCount);
    }
}
