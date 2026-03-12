/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

/**
 * Holds the options that are given to the FUSE command.
 * The FUSE options are dependent on the FUSE method (e.g. RRF/LINEAR).
 * Each FUSE method has its own FuseConfig implementation.
 */
public interface FuseConfig {
    String WEIGHTS = "weights";
}
