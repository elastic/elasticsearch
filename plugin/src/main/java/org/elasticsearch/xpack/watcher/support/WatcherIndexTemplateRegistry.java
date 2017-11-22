/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.template.TemplateUtils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

public class WatcherIndexTemplateRegistry extends AbstractComponent implements ClusterStateListener {

    // history (please add a comment why you increased the version here)
    // version 1: initial
    // version 2: added mappings for jira action
    // version 3: include watch status in history
    // version 6: upgrade to ES 6, removal of _status field
    // version 7: add full exception stack traces for better debugging
    // Note: if you change this, also inform the kibana team around the watcher-ui
    public static final String INDEX_TEMPLATE_VERSION = "7";

    public static final String HISTORY_TEMPLATE_NAME = ".watch-history-" + INDEX_TEMPLATE_VERSION;
    public static final String TRIGGERED_TEMPLATE_NAME = ".triggered_watches";
    public static final String WATCHES_TEMPLATE_NAME = ".watches";

    public static final String[] TEMPLATE_NAMES = new String[] {
            HISTORY_TEMPLATE_NAME, TRIGGERED_TEMPLATE_NAME, WATCHES_TEMPLATE_NAME
    };

    public static final TemplateConfig TEMPLATE_CONFIG_TRIGGERED_WATCHES = new TemplateConfig(TRIGGERED_TEMPLATE_NAME, "triggered-watches");
    public static final TemplateConfig TEMPLATE_CONFIG_WATCH_HISTORY = new TemplateConfig(HISTORY_TEMPLATE_NAME, "watch-history");
    public static final TemplateConfig TEMPLATE_CONFIG_WATCHES = new TemplateConfig(WATCHES_TEMPLATE_NAME, "watches");
    public static final TemplateConfig[] TEMPLATE_CONFIGS = new TemplateConfig[]{
            TEMPLATE_CONFIG_TRIGGERED_WATCHES, TEMPLATE_CONFIG_WATCH_HISTORY, TEMPLATE_CONFIG_WATCHES
    };

    private final Client client;
    private final ThreadPool threadPool;
    private final TemplateConfig[] indexTemplates;
    private final ConcurrentMap<String, AtomicBoolean> templateCreationsInProgress = new ConcurrentHashMap<>();

    public WatcherIndexTemplateRegistry(Settings settings, ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(settings);
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

        // if this node is newer than the master node, we probably need to add the history template, which might be newer than the
        // history template the master node has, so we need potentially add new templates despite being not the master node
        DiscoveryNode localNode = event.state().getNodes().getLocalNode();
        DiscoveryNode masterNode = event.state().getNodes().getMasterNode();
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
                    new ActionListener<PutIndexTemplateResponse>() {
                        @Override
                        public void onResponse(PutIndexTemplateResponse response) {
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
        return state.getMetaData().getTemplates().containsKey(HISTORY_TEMPLATE_NAME) &&
                state.getMetaData().getTemplates().containsKey(TRIGGERED_TEMPLATE_NAME) &&
                state.getMetaData().getTemplates().containsKey(WATCHES_TEMPLATE_NAME);
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
            String template = TemplateUtils.loadTemplate("/" + fileName + ".json", INDEX_TEMPLATE_VERSION,
                    Pattern.quote("${xpack.watcher.template.version}"));
            assert template != null && template.length() > 0;
            return template.getBytes(StandardCharsets.UTF_8);
        }
    }
}
