/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.heuristic;

import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.special.Gamma;

public class MlChiSquaredDistribution {

    private final GammaDistribution gamma;

    public MlChiSquaredDistribution(double degreesOfFreedom) {
        gamma = new GammaDistribution(degreesOfFreedom / 2, 2);
    }

    public double survivalFunction(double x) {
        return x <= 0 ?
            1 :
            Gamma.regularizedGammaQ(gamma.getShape(), x / gamma.getScale());
    }

}
