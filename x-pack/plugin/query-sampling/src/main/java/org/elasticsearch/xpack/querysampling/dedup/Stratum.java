/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

/**
 * The part of the vector space a query is in, which the sampler balances over.
 *
 * @param space   a field and its dimensions; clusters of different spaces are unrelated
 * @param cluster the nearest cluster of the space
 */
public record Stratum(String space, int cluster) {}
