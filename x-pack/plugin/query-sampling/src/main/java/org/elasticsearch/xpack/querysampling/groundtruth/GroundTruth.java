/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;

import java.util.List;

/**
 * The exact top-k of a query when it was computed, best first, with the {@link DataState} (if taken) that tells later if it
 * went out of date.
 */
public record GroundTruth(List<CapturedSearch.Hit> neighbors, @Nullable DataState dataState) {}
