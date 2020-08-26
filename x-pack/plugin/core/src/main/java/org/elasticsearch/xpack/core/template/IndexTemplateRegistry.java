/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.template;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
    protected List<IndexTemplateConfig> getLegacyTemplateConfigs() {
        return Collections.emptyList();
    }

    /**
     * Retrieves return a list of {@link IndexTemplateConfig} that represents
     * the component templates that should be installed and managed. Component
     * templates are always installed prior composable templates, so they may
     * be referenced by a composable template.
     * @return The configurations for the templates that should be installed.
     */
    protected List<IndexTemplateConfig> getComponentTemplateConfigs() {
        return Collections.emptyList();
    }

    /**
     * Retrieves return a list of {@link IndexTemplateConfig} that represents
     * the composable templates that should be installed and managed.
     * @return The configurations for the templates that should be installed.
     */
    protected List<IndexTemplateConfig> getComposableTemplateConfigs() {
        return Collections.emptyList();
    }

    /**
     * Retrieves a list of {@link LifecyclePolicyConfig} that represents the ILM
     * policies that should be installed and managed. Only called if ILM is enabled.
     * @return The configurations for the lifecycle policies that should be installed.
     */
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        return Collections.emptyList();
    }

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

        // This registry requires to run on a master node.
        // If not a master node, exit.
        if (requiresMasterNode() && state.nodes().isLocalNodeElectedMaster() == false) {
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

    /**
     * Whether the registry should only apply changes when running on the master node.
     * This is useful for plugins where certain actions are performed on master nodes
     * and the templates should match the respective version.
     */
    protected boolean requiresMasterNode() {
        return false;
    }

    private void addTemplatesIfMissing(ClusterState state) {
        addLegacyTemplatesIfMissing(state);
        addComponentTemplatesIfMissing(state);
        addComposableTemplatesIfMissing(state);
    }

    private void addLegacyTemplatesIfMissing(ClusterState state) {
        final List<IndexTemplateConfig> indexTemplates = getLegacyTemplateConfigs();
        for (IndexTemplateConfig newTemplate : indexTemplates) {
            final String templateName = newTemplate.getTemplateName();
            final AtomicBoolean creationCheck = templateCreationsInProgress.computeIfAbsent(templateName, key -> new AtomicBoolean(false));
            if (creationCheck.compareAndSet(false, true)) {
                IndexTemplateMetadata currentTemplate = state.metadata().getTemplates().get(templateName);
                if (Objects.isNull(currentTemplate)) {
                    logger.debug("adding legacy template [{}] for [{}], because it doesn't exist", templateName, getOrigin());
                    putLegacyTemplate(newTemplate, creationCheck);
                } else if (Objects.isNull(currentTemplate.getVersion()) || newTemplate.getVersion() > currentTemplate.getVersion()) {
                    // IndexTemplateConfig now enforces templates contain a `version` property, so if the template doesn't have one we can
                    // safely assume it's an old version of the template.
                    logger.info("upgrading legacy template [{}] for [{}] from version [{}] to version [{}]",
                        templateName, getOrigin(), currentTemplate.getVersion(), newTemplate.getVersion());
                    putLegacyTemplate(newTemplate, creationCheck);
                } else {
                    creationCheck.set(false);
                    logger.trace("not adding legacy template [{}] for [{}], because it already exists at version [{}]",
                        templateName, getOrigin(), currentTemplate.getVersion());
                }
            } else {
                logger.trace("skipping the creation of legacy template [{}] for [{}], because its creation is in progress",
                    templateName, getOrigin());
            }
        }
    }

    private void addComponentTemplatesIfMissing(ClusterState state) {
        final List<IndexTemplateConfig> indexTemplates = getComponentTemplateConfigs();
        for (IndexTemplateConfig newTemplate : indexTemplates) {
            final String templateName = newTemplate.getTemplateName();
            final AtomicBoolean creationCheck = templateCreationsInProgress.computeIfAbsent(templateName, key -> new AtomicBoolean(false));
            if (creationCheck.compareAndSet(false, true)) {
                ComponentTemplate currentTemplate = state.metadata().componentTemplates().get(templateName);
                if (Objects.isNull(currentTemplate)) {
                    logger.debug("adding component template [{}] for [{}], because it doesn't exist", templateName, getOrigin());
                    putComponentTemplate(newTemplate, creationCheck);
                } else if (Objects.isNull(currentTemplate.version()) || newTemplate.getVersion() > currentTemplate.version()) {
                    // IndexTemplateConfig now enforces templates contain a `version` property, so if the template doesn't have one we can
                    // safely assume it's an old version of the template.
                    logger.info("upgrading component template [{}] for [{}] from version [{}] to version [{}]",
                        templateName, getOrigin(), currentTemplate.version(), newTemplate.getVersion());
                    putComponentTemplate(newTemplate, creationCheck);
                } else {
                    creationCheck.set(false);
                    logger.trace("not adding component template [{}] for [{}], because it already exists at version [{}]",
                        templateName, getOrigin(), currentTemplate.version());
                }
            } else {
                logger.trace("skipping the creation of component template [{}] for [{}], because its creation is in progress",
                    templateName, getOrigin());
            }
        }
    }

    private void addComposableTemplatesIfMissing(ClusterState state) {
        final List<IndexTemplateConfig> indexTemplates = getComposableTemplateConfigs();
        for (IndexTemplateConfig newTemplate : indexTemplates) {
            final String templateName = newTemplate.getTemplateName();
            final AtomicBoolean creationCheck = templateCreationsInProgress.computeIfAbsent(templateName, key -> new AtomicBoolean(false));
            if (creationCheck.compareAndSet(false, true)) {
                ComposableIndexTemplate currentTemplate = state.metadata().templatesV2().get(templateName);
                boolean componentTemplatesAvailable = componentTemplatesExist(state, newTemplate);
                if (componentTemplatesAvailable == false) {
                    creationCheck.set(false);
                    logger.trace("not adding composable template [{}] for [{}] because its required component templates do not exist",
                        templateName, getOrigin());
                } else if (Objects.isNull(currentTemplate)) {
                    logger.debug("adding composable template [{}] for [{}], because it doesn't exist", templateName, getOrigin());
                    putComposableTemplate(newTemplate, creationCheck);
                } else if (Objects.isNull(currentTemplate.version()) || newTemplate.getVersion() > currentTemplate.version()) {
                    // IndexTemplateConfig now enforces templates contain a `version` property, so if the template doesn't have one we can
                    // safely assume it's an old version of the template.
                    logger.info("upgrading composable template [{}] for [{}] from version [{}] to version [{}]",
                        templateName, getOrigin(), currentTemplate.version(), newTemplate.getVersion());
                    putComposableTemplate(newTemplate, creationCheck);
                } else {
                    creationCheck.set(false);
                    logger.trace("not adding composable template [{}] for [{}], because it already exists at version [{}]",
                        templateName, getOrigin(), currentTemplate.version());
                }
            } else {
                logger.trace("skipping the creation of composable template [{}] for [{}], because its creation is in progress",
                    templateName, getOrigin());
            }
        }
    }

    /**
     * Returns true if the cluster state contains all of the component templates needed by the composable template
     */
    private static boolean componentTemplatesExist(ClusterState state, IndexTemplateConfig composableTemplate) {
        final ComposableIndexTemplate indexTemplate;
        try {
            indexTemplate = ComposableIndexTemplate.parse(JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, composableTemplate.loadBytes()));
        } catch (Exception e) {
            throw new ElasticsearchParseException("unable to parse composable template " + composableTemplate.getTemplateName(), e);
        }
        Set<String> neededComponents = new HashSet<>(indexTemplate.composedOf());
        return state.metadata().componentTemplates().keySet().containsAll(neededComponents);
    }

    private void putLegacyTemplate(final IndexTemplateConfig config, final AtomicBoolean creationCheck) {
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
                            logger.error("error adding legacy template [{}] for [{}], request was not acknowledged",
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

    private void putComponentTemplate(final IndexTemplateConfig config, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            final String templateName = config.getTemplateName();

            PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request(templateName);
            try {
                request.componentTemplate(ComponentTemplate.parse(JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION, config.loadBytes())));
            } catch (Exception e) {
                throw new ElasticsearchParseException("unable to parse component template " + config.getTemplateName(), e);
            }
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), getOrigin(), request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        creationCheck.set(false);
                        if (response.isAcknowledged() == false) {
                            logger.error("error adding component template [{}] for [{}], request was not acknowledged",
                                templateName, getOrigin());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        onPutTemplateFailure(config, e);
                    }
                }, (req, listener) -> client.execute(PutComponentTemplateAction.INSTANCE, req, listener));
        });
    }

    private void putComposableTemplate(final IndexTemplateConfig config, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            final String templateName = config.getTemplateName();

            PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(templateName);
            try {
                request.indexTemplate(ComposableIndexTemplate.parse(JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION, config.loadBytes())));
            } catch (Exception e) {
                throw new ElasticsearchParseException("unable to parse composable template " + config.getTemplateName(), e);
            }
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), getOrigin(), request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        creationCheck.set(false);
                        if (response.isAcknowledged() == false) {
                            logger.error("error adding composable template [{}] for [{}], request was not acknowledged",
                                templateName, getOrigin());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        onPutTemplateFailure(config, e);
                    }
                }, (req, listener) -> client.execute(PutComposableIndexTemplateAction.INSTANCE, req, listener));
        });
    }

    private void addIndexLifecyclePoliciesIfMissing(ClusterState state) {

        Optional<IndexLifecycleMetadata> maybeMeta = Optional.ofNullable(state.metadata().custom(IndexLifecycleMetadata.TYPE));
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
