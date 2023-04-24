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
 * A {@link SingleThreadCollectorManager} that wraps a {@link InternalProfileCollector}.
 * It delegates all the profiling to the generated collector via {@link #getCollectorTree()}.
 */
public final class InternalProfileCollectorManager extends SingleThreadCollectorManager {

    public InternalProfileCollectorManager(InternalProfileCollector collector) {
        super(collector);
    }

    public CollectorResult getCollectorTree() {
        return ((InternalProfileCollector) collector).getCollectorTree();
    }
}
