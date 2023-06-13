/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.elasticsearch.common.lucene.MinimumScoreCollector;

import java.io.IOException;
import java.util.Collection;

public class MinimumScoreCollectorManager implements CollectorManager<MinimumScoreCollector, Void> {

    private final CollectorManager<? extends Collector, Void> innerCollectorManager;
    private final float minimumScore;

    public MinimumScoreCollectorManager(CollectorManager<? extends Collector, Void> collectorManager, float minimumScore) {
        this.innerCollectorManager = collectorManager;
        this.minimumScore = minimumScore;
    }

    @Override
    public MinimumScoreCollector newCollector() throws IOException {
        return new MinimumScoreCollector(innerCollectorManager.newCollector(), minimumScore);
    }

    @Override
    public Void reduce(Collection<MinimumScoreCollector> collectors) throws IOException {
        @SuppressWarnings("unchecked")
        CollectorManager<Collector, Void> cm = (CollectorManager<Collector, Void>) innerCollectorManager;
        return cm.reduce(collectors.stream().map(MinimumScoreCollector::getCollector).toList());
    }
}
