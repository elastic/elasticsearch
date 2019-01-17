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
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyUtils;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
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

    public static final PolicyConfig POLICY_WATCH_HISTORY = new PolicyConfig("watch-history-ilm-policy", "/watch-history-ilm-policy.json");

    private static final Logger logger = LogManager.getLogger(WatcherIndexTemplateRegistry.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final TemplateConfig[] indexTemplates;
    private final NamedXContentRegistry xContentRegistry;
    private final ConcurrentMap<String, AtomicBoolean> templateCreationsInProgress = new ConcurrentHashMap<>();
    private final AtomicBoolean historyPolicyCreationInProgress = new AtomicBoolean();

    public WatcherIndexTemplateRegistry(ClusterService clusterService, ThreadPool threadPool, Client client,
                                        NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.threadPool = threadPool;
        this.indexTemplates = TEMPLATE_CONFIGS;
        this.xContentRegistry = xContentRegistry;
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
            addIndexLifecyclePolicyIfMissing(state);
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

    // Package visible for testing
    LifecyclePolicy loadWatcherHistoryPolicy() {
        return LifecyclePolicyUtils.loadPolicy(POLICY_WATCH_HISTORY.policyName, POLICY_WATCH_HISTORY.fileName, xContentRegistry);
    }

    private void addIndexLifecyclePolicyIfMissing(ClusterState state) {
        if (historyPolicyCreationInProgress.compareAndSet(false, true)) {
            final LifecyclePolicy policyOnDisk = loadWatcherHistoryPolicy();

            Optional<IndexLifecycleMetadata> maybeMeta = Optional.ofNullable(state.metaData().custom(IndexLifecycleMetadata.TYPE));
            final boolean needsUpdating = maybeMeta
                .flatMap(ilmMeta -> Optional.ofNullable(ilmMeta.getPolicies().get(policyOnDisk.getName())))
                .isPresent() == false; // If there is no policy then one needs to be put;

            if (needsUpdating) {
                putPolicy(policyOnDisk, historyPolicyCreationInProgress);
            }
        }
    }

    private void putPolicy(final LifecyclePolicy policy, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            PutLifecycleAction.Request request = new PutLifecycleAction.Request(policy);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), WATCHER_ORIGIN, request,
                new ActionListener<PutLifecycleAction.Response>() {
                    @Override
                    public void onResponse(PutLifecycleAction.Response response) {
                        creationCheck.set(false);
                        if (response.isAcknowledged() == false) {
                            logger.error("error adding watcher index lifecycle policy [{}], request was not acknowledged",
                                policy.getName());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        logger.error(new ParameterizedMessage("error adding watcher index lifecycle policy [{}]",
                            policy.getName()), e);
                    }
                }, (req, listener) -> new XPackClient(client).ilmClient().putLifecyclePolicy(req, listener));
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
    public static class PolicyConfig {

        private final String policyName;
        private String fileName;

        PolicyConfig(String templateName, String fileName) {
            this.policyName = templateName;
            this.fileName = fileName;
        }
    }
}
