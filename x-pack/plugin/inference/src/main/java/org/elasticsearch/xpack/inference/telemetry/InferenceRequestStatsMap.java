/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.inference.Model;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.inference.InferenceRequestStats;

public class InferenceRequestStatsMap extends StatsMap<Model, InferenceAPMStats, InferenceRequestStats> {
    public static InferenceRequestStatsMap of(MeterRegistry meterRegistry) {
        var statsFactory = new InferenceAPMStats.Factory(meterRegistry);
        return new InferenceRequestStatsMap(statsFactory);
    }

    private InferenceRequestStatsMap(InferenceAPMStats.Factory factory) {
        super(InferenceAPMStats::key, factory::newInferenceRequestAPMCounter);
    }
}
