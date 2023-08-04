/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.history;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;

/**
 * Manages the index template and associated ILM policy for the Snapshot
 * Lifecycle Management history index.
 */
public class SnapshotLifecycleTemplateRegistry extends IndexTemplateRegistry {
    // history (please add a comment why you increased the version here)
    // version 1: initial
    // version 2: converted to hidden index
    // version 3: templates moved to composable templates
    // version 4:converted data stream
    // version 5: add `allow_auto_create` setting
    public static final int INDEX_TEMPLATE_VERSION = 5;

    public static final String SLM_TEMPLATE_VERSION_VARIABLE = "xpack.slm.template.version";
    public static final String SLM_TEMPLATE_NAME = ".slm-history";

    public static final String SLM_POLICY_NAME = "slm-history-ilm-policy";

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    private final boolean slmHistoryEnabled;

    public SnapshotLifecycleTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        slmHistoryEnabled = SLM_HISTORY_INDEX_ENABLED_SETTING.get(nodeSettings);
    }

    public static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(SLM_TEMPLATE_NAME, "/slm-history.json", INDEX_TEMPLATE_VERSION, SLM_TEMPLATE_VERSION_VARIABLE)
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        if (slmHistoryEnabled == false) {
            return Map.of();
        }
        return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
    }

    private static final List<LifecyclePolicy> LIFECYCLE_POLICIES = List.of(
        new LifecyclePolicyConfig(SLM_POLICY_NAME, "/slm-history-ilm-policy.json").load(
            new NamedXContentRegistry(xContentEntries())
        )
    );

    private static List<NamedXContentRegistry.Entry> xContentEntries() {
        return Arrays.asList(
            // Custom Metadata
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(IndexLifecycleMetadata.TYPE),
                parser -> IndexLifecycleMetadata.PARSER.parse(parser, null)
            ),
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(SnapshotLifecycleMetadata.TYPE),
                parser -> SnapshotLifecycleMetadata.PARSER.parse(parser, null)
            ),
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(LifecycleOperationMetadata.TYPE),
                parser -> LifecycleOperationMetadata.PARSER.parse(parser, null)
            ),
            // Lifecycle Types
            new NamedXContentRegistry.Entry(
                LifecycleType.class,
                new ParseField(TimeseriesLifecycleType.TYPE),
                (p, c) -> TimeseriesLifecycleType.INSTANCE
            ),
            // Lifecycle Actions
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse),
            new NamedXContentRegistry.Entry(
                LifecycleAction.class,
                new ParseField(WaitForSnapshotAction.NAME),
                WaitForSnapshotAction::parse
            ),
            new NamedXContentRegistry.Entry(
                LifecycleAction.class,
                new ParseField(SearchableSnapshotAction.NAME),
                SearchableSnapshotAction::parse
            ),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MigrateAction.NAME), MigrateAction::parse),
            // TSDB Downsampling
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DownsampleAction.NAME), DownsampleAction::parse)
        );
    }

    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        if (slmHistoryEnabled == false) {
            return Collections.emptyList();
        }
        return LIFECYCLE_POLICIES;
    }

    @Override
    protected String getOrigin() {
        return INDEX_LIFECYCLE_ORIGIN; // TODO use separate SLM origin?
    }

    public boolean validate(ClusterState state) {
        boolean allTemplatesPresent = getComposableTemplateConfigs().keySet()
            .stream()
            .allMatch(name -> state.metadata().templatesV2().containsKey(name));

        Optional<Map<String, LifecyclePolicy>> maybePolicies = Optional.<IndexLifecycleMetadata>ofNullable(
            state.metadata().custom(IndexLifecycleMetadata.TYPE)
        ).map(IndexLifecycleMetadata::getPolicies);
        Set<String> policyNames = getPolicyConfigs().stream().map(LifecyclePolicy::getName).collect(Collectors.toSet());

        boolean allPoliciesPresent = maybePolicies.map(policies -> policies.keySet().containsAll(policyNames)).orElse(false);
        return allTemplatesPresent && allPoliciesPresent;
    }
}
