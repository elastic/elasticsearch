/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.template;

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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Abstracts the logic of managing versioned index templates and lifecycle policies for plugins that require such things.
 */
public abstract class IndexTemplateRegistry implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(IndexTemplateRegistry.class);

    protected final Settings settings;
    protected final Client client;
    protected final ThreadPool threadPool;
    protected final NamedXContentRegistry xContentRegistry;
    protected final ConcurrentMap<String, AtomicBoolean> templateCreationsInProgress = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String, AtomicBoolean> policyCreationsInProgress = new ConcurrentHashMap<>();

    public IndexTemplateRegistry(Settings nodeSettings, ClusterService clusterService, ThreadPool threadPool, Client client,
                                 NamedXContentRegistry xContentRegistry) {
        this.settings = nodeSettings;
        this.client = client;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        clusterService.addListener(this);
    }

    /**
     * Retrieves return a list of {@link IndexTemplateConfig} that represents
     * the index templates that should be installed and managed.
     * @return The configurations for the templates that should be installed.
     */
    protected abstract List<IndexTemplateConfig> getTemplateConfigs();

    /**
     * Retrieves a list of {@link LifecyclePolicyConfig} that represents the ILM
     * policies that should be installed and managed. Only called if ILM is enabled.
     * @return The configurations for the lifecycle policies that should be installed.
     */
    protected abstract List<LifecyclePolicyConfig> getPolicyConfigs();

    /**
     * Retrieves an identifier that is used to identify which plugin is asking for this.
     * @return A string ID for the plugin managing these templates.
     */
    protected abstract String getOrigin();

    /**
     * Called when creation of an index template fails.
     * @param config The template config that failed to be created.
     * @param e The exception that caused the failure.
     */
    protected void onPutTemplateFailure(IndexTemplateConfig config, Exception e) {
        logger.error(new ParameterizedMessage("error adding index template [{}] from [{}] for [{}]",
            config.getTemplateName(), config.getFileName(), getOrigin()), e);
    }

    /**
     * Called when creation of a lifecycle policy fails.
     * @param policy The lifecycle policy that failed to be created.
     * @param e The exception that caused the failure.
     */
    protected void onPutPolicyFailure(LifecyclePolicy policy, Exception e) {
        logger.error(new ParameterizedMessage("error adding lifecycle policy [{}] for [{}]",
            policy.getName(), getOrigin()), e);
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

        // if this node is newer than the master node, we probably need to add the template, which might be newer than the
        // template the master node has, so we need potentially add new templates despite being not the master node
        DiscoveryNode localNode = event.state().getNodes().getLocalNode();
        boolean localNodeVersionAfterMaster = localNode.getVersion().after(masterNode.getVersion());

        if (event.localNodeMaster() || localNodeVersionAfterMaster) {
            addTemplatesIfMissing(state);
            addIndexLifecyclePoliciesIfMissing(state);
        }
    }

    private void addTemplatesIfMissing(ClusterState state) {
        final List<IndexTemplateConfig> indexTemplates = getTemplateConfigs();
        for (IndexTemplateConfig template : indexTemplates) {
            final String templateName = template.getTemplateName();
            final AtomicBoolean creationCheck = templateCreationsInProgress.computeIfAbsent(templateName, key -> new AtomicBoolean(false));
            if (creationCheck.compareAndSet(false, true)) {
                if (!state.metaData().getTemplates().containsKey(templateName)) {
                    logger.debug("adding index template [{}] for [{}], because it doesn't exist", templateName, getOrigin());
                    putTemplate(template, creationCheck);
                } else {
                    creationCheck.set(false);
                    logger.trace("not adding index template [{}] for [{}], because it already exists", templateName, getOrigin());
                }
            } else {
                logger.trace("skipping the creation of index template [{}] for [{}], because its creation is in progress",
                    templateName, getOrigin());
            }
        }
    }

    private void putTemplate(final IndexTemplateConfig config, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            final String templateName = config.getTemplateName();

            PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName).source(config.loadBytes(), XContentType.JSON);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), getOrigin(), request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        creationCheck.set(false);
                        if (response.isAcknowledged() == false) {
                            logger.error("error adding index template [{}] for [{}], request was not acknowledged",
                                templateName, getOrigin());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        onPutTemplateFailure(config, e);
                    }
                }, client.admin().indices()::putTemplate);
        });
    }

    private void addIndexLifecyclePoliciesIfMissing(ClusterState state) {
        boolean ilmSupported = XPackSettings.INDEX_LIFECYCLE_ENABLED.get(settings);

        if (ilmSupported) {
            Optional<IndexLifecycleMetadata> maybeMeta = Optional.ofNullable(state.metaData().custom(IndexLifecycleMetadata.TYPE));
            List<LifecyclePolicy> policies = getPolicyConfigs().stream()
                .map(policyConfig -> policyConfig.load(xContentRegistry))
                .collect(Collectors.toList());

            for (LifecyclePolicy policy : policies) {
                final AtomicBoolean creationCheck = policyCreationsInProgress.computeIfAbsent(policy.getName(),
                    key -> new AtomicBoolean(false));
                if (creationCheck.compareAndSet(false, true)) {
                    final boolean policyNeedsToBeCreated = maybeMeta
                        .flatMap(ilmMeta -> Optional.ofNullable(ilmMeta.getPolicies().get(policy.getName())))
                        .isPresent() == false;
                    if (policyNeedsToBeCreated) {
                        logger.debug("adding lifecycle policy [{}] for [{}], because it doesn't exist", policy.getName(), getOrigin());
                        putPolicy(policy, creationCheck);
                    } else {
                        logger.trace("not adding lifecycle policy [{}] for [{}], because it already exists",
                            policy.getName(), getOrigin());
                        creationCheck.set(false);
                    }
                }
            }
        }
    }

    private void putPolicy(final LifecyclePolicy policy, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            PutLifecycleAction.Request request = new PutLifecycleAction.Request(policy);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), getOrigin(), request,
                new ActionListener<PutLifecycleAction.Response>() {
                    @Override
                    public void onResponse(PutLifecycleAction.Response response) {
                        creationCheck.set(false);
                        if (response.isAcknowledged() == false) {
                            logger.error("error adding lifecycle policy [{}] for [{}], request was not acknowledged",
                                policy.getName(), getOrigin());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        onPutPolicyFailure(policy, e);
                    }
                }, (req, listener) -> client.execute(PutLifecycleAction.INSTANCE, req, listener));
        });
    }

}
