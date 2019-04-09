/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.enrich.GetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportGetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestGenEnrichPolicyAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.core.XPackSettings.ENRICH_ENABLED_SETTING;

public class EnrichPlugin extends Plugin implements ActionPlugin, IngestPlugin {

    private final Settings settings;
    private final Boolean enabled;

    public EnrichPlugin(final Settings settings) {
        this.settings = settings;
        this.enabled = ENRICH_ENABLED_SETTING.get(settings);
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.emptyMap();
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return emptyList();
        }

        return Arrays.asList(
            new ActionHandler<>(GetEnrichPolicyAction.INSTANCE, TransportGetEnrichPolicyAction.class)
        );
    }


    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled == false) {
            return emptyList();
        }

        return Arrays.asList(
            new RestGenEnrichPolicyAction(settings, restController)
        );
    }
}
