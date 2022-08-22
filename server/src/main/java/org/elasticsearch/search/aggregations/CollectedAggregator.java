/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.core.Releasable;

/**
 * Stores the collected output of an aggregator, which can then be serialized back to the
 * coordinating node for merging.  This replaces the old {@link Aggregation} and {@link InternalAggregation}
 * classes.  There are two key differences:
 *
 * There should be exactly one {@link CollectedAggregator} per {@link Aggregator}, which is to say
 * one per shard per aggregation.
 *
 * The {@link CollectedAggregator} is {@link Releasable}.  This allows it to manage memory for data
 * structures that live longer than the collection phase.
 */
public abstract class CollectedAggregator implements Releasable, VersionedNamedWriteable {

}
