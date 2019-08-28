/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;
import org.elasticsearch.xpack.analytics.action.AnalyticsInfoTransportAction;
import org.elasticsearch.xpack.analytics.action.AnalyticsUsageTransportAction;
import org.elasticsearch.xpack.analytics.action.TransportAnalyticsStatsAction;
import org.elasticsearch.xpack.analytics.cumulativecardinality.CumulativeCardinalityPipelineAggregationBuilder;
import org.elasticsearch.xpack.analytics.cumulativecardinality.CumulativeCardinalityPipelineAggregator;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonList;

public class AnalyticsPlugin extends Plugin implements SearchPlugin, ActionPlugin {

    // TODO this should probably become more structured once Analytics plugin has more than just one agg
    public static AtomicLong cumulativeCardUsage = new AtomicLong(0);

    public AnalyticsPlugin() { }

    public static XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        return singletonList(new PipelineAggregationSpec(
            CumulativeCardinalityPipelineAggregationBuilder.NAME,
            CumulativeCardinalityPipelineAggregationBuilder::new,
            CumulativeCardinalityPipelineAggregator::new,
            CumulativeCardinalityPipelineAggregationBuilder::parse));
    }

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(XPackUsageFeatureAction.ANALYTICS, AnalyticsUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.ANALYTICS, AnalyticsInfoTransportAction.class),
            new ActionHandler<>(AnalyticsStatsAction.INSTANCE, TransportAnalyticsStatsAction.class));
    }
}
