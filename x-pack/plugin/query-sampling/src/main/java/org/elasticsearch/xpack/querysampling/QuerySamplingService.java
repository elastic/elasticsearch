/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import java.util.function.Supplier;

/** Exposes the node-local sampling stats to the stats action; nodes do not coordinate, state is combined on read. */
public record QuerySamplingService(Supplier<QuerySamplingStats> stats) {}
