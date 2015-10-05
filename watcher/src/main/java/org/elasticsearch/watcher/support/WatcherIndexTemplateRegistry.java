/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.watch.WatchStore;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

/**
 */
public class WatcherIndexTemplateRegistry extends AbstractComponent implements ClusterStateListener, NodeSettingsService.Listener {
    private static final String FORBIDDEN_INDEX_SETTING = "index.mapper.dynamic";

    private final ClientProxy client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Set<TemplateConfig> indexTemplates;

    private volatile Map<String, Settings> customIndexSettings;

    @Inject
    public WatcherIndexTemplateRegistry(Settings settings, NodeSettingsService nodeSettingsService, ClusterService clusterService,
                                        ThreadPool threadPool, ClientProxy client, Set<TemplateConfig> configs) {
        super(settings);
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indexTemplates = unmodifiableSet(new HashSet<>(configs));
        clusterService.add(this);
        nodeSettingsService.addListener(this);

        Map<String, Settings> customIndexSettings = new HashMap<>();
        for (TemplateConfig indexTemplate : indexTemplates) {
            Settings customSettings = this.settings.getAsSettings(indexTemplate.getSettingsPrefix());
            customIndexSettings.put(indexTemplate.getSettingsPrefix(), customSettings);
        }
        this.customIndexSettings = unmodifiableMap(customIndexSettings);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have the index templates,
            // while they actually do exist
            return;
        }

        if (event.localNodeMaster() == false) {
            // Only the node that runs or will run Watcher should update the templates. Otherwise unnecessary put template
            // calls would happen
            return;
        }

        addTemplatesIfMissing(state, false);
    }

    /**
     * Adds the registered index templates if missing to the cluster.
     */
    public void addTemplatesIfMissing() {
        // to be sure that the templates exist after this method call, we should wait until the put index templates calls
        // have returned if the templates were missing
        addTemplatesIfMissing(clusterService.state(), true);
    }

    void addTemplatesIfMissing(ClusterState state, boolean wait) {
        for (TemplateConfig template : indexTemplates) {
            if (!state.metaData().getTemplates().containsKey(template.getTemplateName())) {
                logger.debug("adding index template [{}], because it doesn't exist", template.getTemplateName());
                putTemplate(template, wait);
            } else {
                logger.trace("not adding index template [{}], because it already exists", template.getTemplateName());
            }
        }
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        if (clusterService.localNode().masterNode() == false) {
            // Only the node that runs or will run Watcher should update the templates. Otherwise unnecessary put template
            // calls would happen
            return;
        }

        for (TemplateConfig config : indexTemplates) {
            Settings newSettings = Settings.builder()
                    .put(settings.getAsSettings(config.getSettingsPrefix()))
                    .build();
            if (newSettings.names().isEmpty()) {
                continue;
            }

            Settings existingSettings = customIndexSettings.get(config.getSettingsPrefix());
            if (existingSettings == null) {
                existingSettings = Settings.EMPTY;
            }

            boolean changed = false;
            Settings.Builder builder = Settings.builder().put(existingSettings);
            for (Map.Entry<String, String> newSettingsEntry : newSettings.getAsMap().entrySet()) {
                String name = "index." + newSettingsEntry.getKey();
                if (FORBIDDEN_INDEX_SETTING.equals(name)) {
                    logger.warn("overriding the default [{}} setting is forbidden. ignoring...", name);
                    continue;
                }

                String newValue = newSettingsEntry.getValue();
                String currentValue = existingSettings.get(name);
                if (!newValue.equals(currentValue)) {
                    changed = true;
                    builder.put(name, newValue);
                    logger.info("changing setting [{}] from [{}] to [{}]", name, currentValue, newValue);
                }
            }

            if (changed) {
                Map<String, Settings> customIndexSettings = new HashMap<String, Settings>(this.customIndexSettings);
                customIndexSettings.put(config.getSettingsPrefix(), builder.build());
                this.customIndexSettings = customIndexSettings;
                putTemplate(config, false);
            }
        }
    }

    private void putTemplate(final TemplateConfig config, boolean wait) {
        final Executor executor;
        if (wait) {
            executor = Runnable::run;
        } else {
            executor = threadPool.generic();
        }
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try (InputStream is = WatchStore.class.getResourceAsStream("/" + config.getTemplateName()+ ".json")) {
                    if (is == null) {
                        logger.error("Resource [/" + config.getTemplateName() + ".json] not found in classpath");
                        return;
                    }
                    final byte[] template;
                    try (BytesStreamOutput out = new BytesStreamOutput()) {
                        Streams.copy(is, out);
                        template = out.bytes().toBytes();
                    }

                    PutIndexTemplateRequest request = new PutIndexTemplateRequest(config.getTemplateName()).source(template);
                    Settings customSettings = customIndexSettings.get(config.getSettingsPrefix());
                    if (customSettings != null && customSettings.names().size() > 0) {
                        Settings updatedSettings = Settings.builder()
                                .put(request.settings())
                                .put(customSettings)
                                .build();
                        request.settings(updatedSettings);
                    }
                    PutIndexTemplateResponse response = client.putTemplate(request);
                } catch (Exception e) {
                    logger.error("failed to load [{}.json]", e, config.getTemplateName());
                }
            }
        });
    }

    public static class TemplateConfig {

        private final String templateName;
        private final String settingsPrefix;

        public TemplateConfig(String templateName, String settingsPrefix) {
            this.templateName = templateName;
            this.settingsPrefix = settingsPrefix;
        }

        public String getTemplateName() {
            return templateName;
        }

        public String getSettingsPrefix() {
            return settingsPrefix;
        }

        public String getDynamicSettingsPrefix() {
            return settingsPrefix + ".*";
        }
    }
}
