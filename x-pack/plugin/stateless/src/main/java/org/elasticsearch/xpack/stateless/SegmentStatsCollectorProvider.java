/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.index.codec.SegmentStatsCollector;

import java.util.List;

/**
 * Extension point for plugins extending the stateless plugin to register {@link SegmentStatsCollector}s. The collectors are
 * passed to the codecs writing segments of every index on this node and observe doc values as segments are flushed and
 * merged; whether a collector applies to a particular index is decided by {@link SegmentStatsCollector#appliesTo}.
 */
public interface SegmentStatsCollectorProvider {

    /**
     * Returns the collectors to register, never {@code null}.
     */
    List<SegmentStatsCollector> getSegmentStatsCollectors();
}
