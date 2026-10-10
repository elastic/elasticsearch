/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

/**
 * The part of the vector space a query is in, which is what the sampler balances the sample over.
 *
 * @param space   what the vectors are of, a field and its dimensions: clusters of different spaces have nothing to do with each other
 * @param cluster the cluster of the space that the query is nearest to
 */
public record Stratum(String space, int cluster) {}
