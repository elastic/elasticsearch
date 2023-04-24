/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.search.query.SingleThreadCollectorManager;

/**
 * This class wraps a Lucene Collector Manager. It assumes execution on a single thread
 * so it delegates all the profiling to the generated collector via {@link #getCollectorTree()}.
 */
public final class InternalProfileCollectorManager extends SingleThreadCollectorManager<InternalProfileCollector> {

    public InternalProfileCollectorManager(InternalProfileCollector collector) {
        super(collector);
    }

    public CollectorResult getCollectorTree() {
        return collector.getCollectorTree();
    }
}
