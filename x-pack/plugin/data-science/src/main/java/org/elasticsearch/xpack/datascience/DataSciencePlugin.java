/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.datascience;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.datascience.action.DataScienceStatsAction;
import org.elasticsearch.xpack.datascience.action.DataScienceInfoTransportAction;
import org.elasticsearch.xpack.datascience.action.DataScienceUsageTransportAction;
import org.elasticsearch.xpack.datascience.action.TransportDataScienceStatsAction;
import org.elasticsearch.xpack.datascience.cumulativecardinality.CumulativeCardinalityPipelineAggregationBuilder;
import org.elasticsearch.xpack.datascience.cumulativecardinality.CumulativeCardinalityPipelineAggregator;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonList;

public class DataSciencePlugin extends Plugin implements SearchPlugin, ActionPlugin {

    // TODO this should probably become more structured once DataScience plugin has more than just one agg
    public static AtomicLong cumulativeCardUsage = new AtomicLong(0);

    public DataSciencePlugin() { }

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
            new ActionHandler<>(XPackUsageFeatureAction.DATA_SCIENCE, DataScienceUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.DATA_SCIENCE, DataScienceInfoTransportAction.class),
            new ActionHandler<>(DataScienceStatsAction.INSTANCE, TransportDataScienceStatsAction.class));
    }
}
