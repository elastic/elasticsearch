/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class WatcherIndexTemplateRegistry implements ClusterStateListener {

    public static final TemplateConfig TEMPLATE_CONFIG_TRIGGERED_WATCHES = new TemplateConfig(
            WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME, "triggered-watches");
    public static final TemplateConfig TEMPLATE_CONFIG_WATCH_HISTORY = new TemplateConfig(
            WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME, "watch-history");
    public static final TemplateConfig TEMPLATE_CONFIG_WATCHES = new TemplateConfig(
            WatcherIndexTemplateRegistryField.WATCHES_TEMPLATE_NAME, "watches");
    public static final TemplateConfig[] TEMPLATE_CONFIGS = new TemplateConfig[]{
            TEMPLATE_CONFIG_TRIGGERED_WATCHES, TEMPLATE_CONFIG_WATCH_HISTORY, TEMPLATE_CONFIG_WATCHES
    };

    private static final Logger logger = LogManager.getLogger(WatcherIndexTemplateRegistry.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final TemplateConfig[] indexTemplates;
    private final ConcurrentMap<String, AtomicBoolean> templateCreationsInProgress = new ConcurrentHashMap<>();

    public WatcherIndexTemplateRegistry(ClusterService clusterService, ThreadPool threadPool, Client client) {
        this.client = client;
        this.threadPool = threadPool;
        this.indexTemplates = TEMPLATE_CONFIGS;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have the index templates,
            // while they actually do exist
            return;
        }

        // no master node, exit immediately
        DiscoveryNode masterNode = event.state().getNodes().getMasterNode();
        if (masterNode == null) {
            return;
        }

        // if this node is newer than the master node, we probably need to add the history template, which might be newer than the
        // history template the master node has, so we need potentially add new templates despite being not the master node
        DiscoveryNode localNode = event.state().getNodes().getLocalNode();
        boolean localNodeVersionAfterMaster = localNode.getVersion().after(masterNode.getVersion());

        if (event.localNodeMaster() || localNodeVersionAfterMaster) {
            addTemplatesIfMissing(state);
        }
    }

    private void addTemplatesIfMissing(ClusterState state) {
        for (TemplateConfig template : indexTemplates) {
            final String templateName = template.getTemplateName();
            final AtomicBoolean creationCheck = templateCreationsInProgress.computeIfAbsent(templateName, key -> new AtomicBoolean(false));
            if (creationCheck.compareAndSet(false, true)) {
                if (!state.metaData().getTemplates().containsKey(templateName)) {
                    logger.debug("adding index template [{}], because it doesn't exist", templateName);
                    putTemplate(template, creationCheck);
                } else {
                    creationCheck.set(false);
                    logger.trace("not adding index template [{}], because it already exists", templateName);
                }
            }
        }
    }

    private void putTemplate(final TemplateConfig config, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            final String templateName = config.getTemplateName();

            PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName).source(config.load(), XContentType.JSON);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), WATCHER_ORIGIN, request,
                    new ActionListener<AcknowledgedResponse>() {
                        @Override
                        public void onResponse(AcknowledgedResponse response) {
                            creationCheck.set(false);
                            if (response.isAcknowledged() == false) {
                                logger.error("Error adding watcher template [{}], request was not acknowledged", templateName);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            creationCheck.set(false);
                            logger.error(new ParameterizedMessage("Error adding watcher template [{}]", templateName), e);
                        }
                    }, client.admin().indices()::putTemplate);
        });
    }

    public static boolean validate(ClusterState state) {
        return state.getMetaData().getTemplates().containsKey(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME) &&
                state.getMetaData().getTemplates().containsKey(WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME) &&
                state.getMetaData().getTemplates().containsKey(WatcherIndexTemplateRegistryField.WATCHES_TEMPLATE_NAME);
    }

    public static class TemplateConfig {

        private final String templateName;
        private String fileName;

        TemplateConfig(String templateName, String fileName) {
            this.templateName = templateName;
            this.fileName = fileName;
        }

        public String getFileName() {
            return fileName;
        }

        public String getTemplateName() {
            return templateName;
        }

        public byte[] load() {
            String template = TemplateUtils.loadTemplate("/" + fileName + ".json", WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
                    Pattern.quote("${xpack.watcher.template.version}"));
            assert template != null && template.length() > 0;
            return template.getBytes(StandardCharsets.UTF_8);
        }
    }
}
