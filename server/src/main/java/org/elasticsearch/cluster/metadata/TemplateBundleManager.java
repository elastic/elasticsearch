/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.gateway.GatewayService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class TemplateBundleManager implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(TemplateBundleManager.class);

    private final ClusterService clusterService;
    private final TemplateBundles templateBundles;
    private final MetadataIndexTemplateService metadataIndexTemplateService;
    private final AtomicBoolean installing = new AtomicBoolean(false);

    public TemplateBundleManager(
        ClusterService clusterService,
        TemplateBundles templateBundles,
        MetadataIndexTemplateService metadataIndexTemplateService
    ) {
        this.clusterService = clusterService;
        this.templateBundles = templateBundles;
        this.metadataIndexTemplateService = metadataIndexTemplateService;
    }

    void checkAndInstallTemplates(ClusterState state) {
        var componentTemplates = getComponentTemplates(state);
        var composableIndexTemplates = getComposableTemplates(state);
        if (componentTemplates.isEmpty() == false || composableIndexTemplates.isEmpty() == false) {
            if (installing.compareAndSet(false, true)) {
                installTemplates(componentTemplates, composableIndexTemplates);
            }
        }
    }

    void installTemplates(
        List<Map.Entry<String, ComponentTemplate>> componentTemplatesToInstall,
        List<Map.Entry<String, ComposableIndexTemplate>> composableIndexTemplates
    ) {
        clusterService.submitStateUpdateTask("install_templates", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                if (componentTemplatesToInstall.isEmpty() == false) {
                    List<String> installedComponentTemplates = new ArrayList<>(componentTemplatesToInstall.size());
                    for (var entry : componentTemplatesToInstall) {
                        currentState = metadataIndexTemplateService.addComponentTemplate(
                            currentState,
                            false,
                            entry.getKey(),
                            entry.getValue(),
                            false
                        );
                        installedComponentTemplates.add(entry.getKey());
                    }
                    LOGGER.info("installed component templates {}", installedComponentTemplates);
                }

                // Some composable index templates may not be found because none of the component templates it uses existed in cluster state
                // so if there are component templates installed then attempt to find composable index templates to install another time:
                var composableIndexTemplatesToInstall = composableIndexTemplates;
                if (componentTemplatesToInstall.isEmpty() == false) {
                    composableIndexTemplatesToInstall = getComposableTemplates(currentState);
                }

                if (composableIndexTemplatesToInstall.isEmpty() == false) {
                    List<String> installedComposableTemplates = new ArrayList<>(composableIndexTemplatesToInstall.size());
                    for (var entry : composableIndexTemplatesToInstall) {
                        currentState = metadataIndexTemplateService.addIndexTemplateV2(
                            currentState,
                            false,
                            entry.getKey(),
                            entry.getValue(),
                            false
                        );
                        installedComposableTemplates.add(entry.getKey());
                    }
                    LOGGER.info("installed composable index templates {}", installedComposableTemplates);
                }

                return currentState;
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                installing.set(false);
            }

            @Override
            public void onFailure(Exception e) {
                installing.set(false);
                LOGGER.error("error during installing templates", e);
            }
        }, newExecutor());
    }

    List<Map.Entry<String, ComponentTemplate>> getComponentTemplates(ClusterState state) {
        List<Map.Entry<String, ComponentTemplate>> templatesToInstall = new ArrayList<>();
        for (var plugin : templateBundles.templateBundles()) {
            for (var builtinTemplate : plugin.getComponentTemplates().entrySet()) {
                final var templateName = builtinTemplate.getKey();
                var currentTemplate = state.metadata().componentTemplates().get(templateName);
                if (currentTemplate == null) {
                    LOGGER.debug("adding component template [{}] for [{}], because it doesn't exist", templateName, plugin.getName());
                    templatesToInstall.add(builtinTemplate);
                } else if (Objects.isNull(currentTemplate.version()) || builtinTemplate.getValue().version() > currentTemplate.version()) {
                    // IndexTemplateConfig now enforces templates contain a `version` property, so if the template doesn't have one we can
                    // safely assume it's an old version of the template.
                    LOGGER.info(
                        "upgrading component template [{}] for [{}] from version [{}] to version [{}]",
                        templateName,
                        plugin.getName(),
                        currentTemplate.version(),
                        builtinTemplate.getValue().version()
                    );
                    templatesToInstall.add(builtinTemplate);
                } else {
                    LOGGER.trace(
                        "not adding component template [{}] for [{}], because it already exists at version [{}]",
                        templateName,
                        plugin.getName(),
                        currentTemplate.version()
                    );
                }
            }
        }
        return templatesToInstall;
    }

    List<Map.Entry<String, ComposableIndexTemplate>> getComposableTemplates(ClusterState state) {
        List<Map.Entry<String, ComposableIndexTemplate>> templatesToInstall = new ArrayList<>();
        for (var plugin : templateBundles.templateBundles()) {
            for (var builtinTemplate : plugin.getComposableIndexTemplates().entrySet()) {
                final String templateName = builtinTemplate.getKey();
                ComposableIndexTemplate currentTemplate = state.metadata().templatesV2().get(templateName);
                boolean componentTemplatesAvailable = componentTemplatesExist(state, builtinTemplate.getValue());
                if (componentTemplatesAvailable == false) {
                    LOGGER.trace(
                        "not adding composable template [{}] for [{}] because its required component templates do not exist",
                        templateName,
                        plugin.getName()
                    );
                } else if (Objects.isNull(currentTemplate)) {
                    LOGGER.debug("adding composable template [{}] for [{}], because it doesn't exist", templateName, plugin.getName());
                    templatesToInstall.add(builtinTemplate);
                } else if (Objects.isNull(currentTemplate.version()) || builtinTemplate.getValue().version() > currentTemplate.version()) {
                    // IndexTemplateConfig now enforces templates contain a `version` property, so if the template doesn't have one we can
                    // safely assume it's an old version of the template.
                    LOGGER.info(
                        "upgrading composable template [{}] for [{}] from version [{}] to version [{}]",
                        templateName,
                        plugin.getName(),
                        currentTemplate.version(),
                        builtinTemplate.getValue().version()
                    );
                    templatesToInstall.add(builtinTemplate);
                } else {
                    LOGGER.trace(
                        "not adding composable template [{}] for [{}], because it already exists at version [{}]",
                        templateName,
                        plugin.getName(),
                        currentTemplate.version()
                    );
                }
            }
        }
        return templatesToInstall;
    }

    private static boolean componentTemplatesExist(ClusterState state, ComposableIndexTemplate indexTemplate) {
        return state.metadata().componentTemplates().keySet().containsAll(indexTemplate.composedOf());
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have the templates,
            // while they actually do exist
            return;
        }

        // no master node, exit immediately
        DiscoveryNode masterNode = event.state().getNodes().getMasterNode();
        if (masterNode == null) {
            return;
        }

        // This requires to run on a master node.
        // If not a master node, exit.
        if (event.localNodeMaster() == false) {
            return;
        }

        checkAndInstallTemplates(state);
    }

    @SuppressForbidden(reason = "installation of templates already happens in batch mode")
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }
}
