/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

import java.util.Map;

public record RrfConfig(Double rankConstant, Map<String, Double> weights) implements FuseConfig {
    public static String RANK_CONSTANT = "rank_constant";
    public static final Double DEFAULT_RANK_CONSTANT = 60d;
    public static RrfConfig DEFAULT_CONFIG = new RrfConfig(DEFAULT_RANK_CONSTANT, Map.of());
}
