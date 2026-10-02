/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;

import java.util.List;

/**
 * The true nearest neighbours of a query at the time they were computed, in rank order. Comparing them with
 * the hits the live search returned tells how much of the true answer the approximate search found.
 *
 * @param neighbors the exact top-k, best first
 */
public record GroundTruth(List<CapturedSearch.Hit> neighbors) {}
