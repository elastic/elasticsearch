/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

import java.util.Map;

public record LinearConfig(Normalizer normalizer, Map<String, Double> weights) implements FuseConfig {
    public enum Normalizer {
        NONE,
        L2_NORM,
        MINMAX
    }

    public static String NORMALIZER = "normalizer";
    public static LinearConfig DEFAULT_CONFIG = new LinearConfig(Normalizer.NONE, Map.of());
}
