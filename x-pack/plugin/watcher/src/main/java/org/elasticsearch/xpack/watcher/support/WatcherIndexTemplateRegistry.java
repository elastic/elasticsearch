/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;
import org.elasticsearch.xpack.watcher.Watcher;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;

public class WatcherIndexTemplateRegistry extends IndexTemplateRegistry {

    public static final String WATCHER_TEMPLATE_VERSION_VARIABLE = "xpack.watcher.template.version";
    public static final IndexTemplateConfig TEMPLATE_CONFIG_TRIGGERED_WATCHES = new IndexTemplateConfig(
        WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME,
        "/triggered-watches.json",
        WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
        WATCHER_TEMPLATE_VERSION_VARIABLE);
    public static final IndexTemplateConfig TEMPLATE_CONFIG_WATCH_HISTORY = new IndexTemplateConfig(
        WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME,
        "/watch-history.json",
        WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
        WATCHER_TEMPLATE_VERSION_VARIABLE);
    public static final IndexTemplateConfig TEMPLATE_CONFIG_WATCH_HISTORY_NO_ILM = new IndexTemplateConfig(
        WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME_NO_ILM,
        "/watch-history-no-ilm.json",
        WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
        WATCHER_TEMPLATE_VERSION_VARIABLE);
    public static final IndexTemplateConfig TEMPLATE_CONFIG_WATCHES = new IndexTemplateConfig(
        WatcherIndexTemplateRegistryField.WATCHES_TEMPLATE_NAME,
        "/watches.json",
        WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
        WATCHER_TEMPLATE_VERSION_VARIABLE);

    public static final LifecyclePolicyConfig POLICY_WATCH_HISTORY = new LifecyclePolicyConfig("watch-history-ilm-policy",
        "/watch-history-ilm-policy.json");

    private final List<IndexTemplateConfig> templatesToUse;

    public WatcherIndexTemplateRegistry(Settings nodeSettings, ClusterService clusterService, ThreadPool threadPool, Client client,
                                        NamedXContentRegistry xContentRegistry) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        boolean ilmManagementEnabled = Watcher.USE_ILM_INDEX_MANAGEMENT.get(nodeSettings);
        templatesToUse = Arrays.asList(
            ilmManagementEnabled ? TEMPLATE_CONFIG_WATCH_HISTORY : TEMPLATE_CONFIG_WATCH_HISTORY_NO_ILM,
            TEMPLATE_CONFIG_TRIGGERED_WATCHES,
            TEMPLATE_CONFIG_WATCHES
        );
    }

    @Override
    protected List<IndexTemplateConfig> getTemplateConfigs() {
        return templatesToUse;
    }

    /**
     * If Watcher is configured not to use ILM, we don't return a policy.
     */
    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        if (Watcher.USE_ILM_INDEX_MANAGEMENT.get(settings) == false) {
            return Collections.emptyList();
        }
        return Collections.singletonList(POLICY_WATCH_HISTORY);
    }

    @Override
    protected String getOrigin() {
        return WATCHER_ORIGIN;
    }

    public static boolean validate(ClusterState state) {
        return (state.getMetadata().getTemplates().containsKey(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME) ||
            state.getMetadata().getTemplates().containsKey(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME_NO_ILM)) &&
            state.getMetadata().getTemplates().containsKey(WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME) &&
            state.getMetadata().getTemplates().containsKey(WatcherIndexTemplateRegistryField.WATCHES_TEMPLATE_NAME);
    }

}
