/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry.Entry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class RankEvalPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(RankEvalAction.INSTANCE, TransportRankEvalAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return Collections.singletonList(new RestRankEvalAction());
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetric.class, PrecisionAtK.NAME, PrecisionAtK::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetric.class, RecallAtK.NAME, RecallAtK::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetric.class, MeanReciprocalRank.NAME, MeanReciprocalRank::new));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(EvaluationMetric.class, DiscountedCumulativeGain.NAME, DiscountedCumulativeGain::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(EvaluationMetric.class, ExpectedReciprocalRank.NAME, ExpectedReciprocalRank::new)
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(MetricDetail.class, PrecisionAtK.NAME, PrecisionAtK.Detail::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(MetricDetail.class, RecallAtK.NAME, RecallAtK.Detail::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(MetricDetail.class, MeanReciprocalRank.NAME, MeanReciprocalRank.Detail::new));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(MetricDetail.class, DiscountedCumulativeGain.NAME, DiscountedCumulativeGain.Detail::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(MetricDetail.class, ExpectedReciprocalRank.NAME, ExpectedReciprocalRank.Detail::new)
        );
        return namedWriteables;
    }

    @Override
    public List<Entry> getNamedXContent() {
        return new RankEvalNamedXContentProvider().getNamedXContentParsers();
    }
}
