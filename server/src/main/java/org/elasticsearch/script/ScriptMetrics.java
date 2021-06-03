/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.metrics.CounterMetric;

public class ScriptMetrics {
    final CounterMetric compilationsMetric = new CounterMetric();
    final CounterMetric cacheEvictionsMetric = new CounterMetric();
    final CounterMetric compilationLimitTriggered = new CounterMetric();

    public void onCompilation() {
        compilationsMetric.inc();
    }

    public void onCacheEviction() {
        cacheEvictionsMetric.inc();
    }

    public void onCompilationLimit() {
        compilationLimitTriggered.inc();
    }

    public ScriptContextStats stats(String context) {
        return new ScriptContextStats(
            context,
            compilationsMetric.count(),
            cacheEvictionsMetric.count(),
            compilationLimitTriggered.count()
        );
    }}
