/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import org.elasticsearch.geometry.Line;

public class GeometrySimplifierSphericalHeightAndBackpathDistanceTests extends GeometrySimplifierTests {
    @Override
    protected SimplificationErrorCalculator calculator() {
        return SimplificationErrorCalculator.SPHERICAL_HEIGHT_AND_BACKPATH_DISTANCE;
    }

    protected void assertLineWithNarrowSpikes(Line simplified, int spikeCount) {
        // The frechet distance can save all spikes
        assertLineSpikes("SphericalFrechet", simplified, spikeCount);
    }
}
