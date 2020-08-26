/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
