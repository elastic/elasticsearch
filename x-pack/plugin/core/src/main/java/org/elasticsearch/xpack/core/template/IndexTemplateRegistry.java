/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.isDataStreamsLifecycleOnlyMode;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Abstracts the logic of managing versioned index templates, ingest pipelines and lifecycle policies for plugins that require such things.
 */
public abstract class IndexTemplateRegistry implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(IndexTemplateRegistry.class);

    protected final Settings settings;
    protected final Client client;
    protected final ThreadPool threadPool;
    protected final NamedXContentRegistry xContentRegistry;
    protected final ClusterService clusterService;
    protected final ConcurrentMap<String, AtomicBoolean> templateCreationsInProgress = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String, AtomicBoolean> policyCreationsInProgress = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String, AtomicBoolean> pipelineCreationsInProgress = new ConcurrentHashMap<>();
    protected final List<LifecyclePolicy> lifecyclePolicies;

    @SuppressWarnings("this-escape")
    public IndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        this.settings = nodeSettings;
        this.client = client;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        this.clusterService = clusterService;
        if (isDataStreamsLifecycleOnlyMode(clusterService.getSettings()) == false) {
            this.lifecyclePolicies = getLifecycleConfigs().stream()
                .map(config -> config.load(LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY))
                .toList();
        } else {
            this.lifecyclePolicies = List.of();
        }
    }

    /**
     * Returns the configured configurations for the lifecycle policies. Subclasses should provide
     * the ILM configurations and they will be loaded if we're not running data stream only mode (controlled via
     * {@link org.elasticsearch.cluster.metadata.DataStreamLifecycle#DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME}).
     *
     * The loaded lifecycle configurations will be installed if returned by {@link #getLifecyclePolicies()}. Child classes
     * have a chance to override {@link #getLifecyclePolicies()} in case they want additional control over if these
     * policies should be installed or not (say, if they belong to functionalities that can be enabled/disabled via a flag).
     *
     * @return The lifecycle policies configurations that pertain to this template registry.
     */
    protected List<LifecyclePolicyConfig> getLifecycleConfigs() {
        return List.of();
    }

    /**
     * Initialize the template registry, adding it as a listener so templates will be installed as necessary
     */
    public void initialize() {
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
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        return Map.of();
    }

    /**
     * Retrieves return a list of {@link IndexTemplateConfig} that represents
     * the composable templates that should be installed and managed.
     * @return The configurations for the templates that should be installed.
     */
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return Map.of();
    }

    /**
     * Retrieves a list of {@link LifecyclePolicy} that represents the ILM
     * policies that should be installed and managed. Only called if ILM is enabled.
     * @return The lifecycle policies that should be installed.
     */
    protected List<LifecyclePolicy> getLifecyclePolicies() {
        return lifecyclePolicies;
    }

    /**
     * Retrieves a list of {@link IngestPipelineConfig} that represents the ingest pipelines
     * that should be installed and managed.
     * @return The configurations for ingest pipelines that should be installed.
     */
    protected List<IngestPipelineConfig> getIngestPipelines() {
        return Collections.emptyList();
    }

    /**
     * Retrieves an identifier that is used to identify which plugin is asking for this.
     * @return A string ID for the plugin managing these templates.
     */
    protected abstract String getOrigin();

    /**
     * Called when creation of an index template fails.
     * @param templateName the template name that failed to be created.
     * @param e The exception that caused the failure.
     */
    protected void onPutTemplateFailure(String templateName, Exception e) {
        logger.error(() -> format("error adding index template [%s] for [%s]", templateName, getOrigin()), e);
    }

    /**
     * Called when creation of a lifecycle policy fails.
     * @param policy The lifecycle policy that failed to be created.
     * @param e The exception that caused the failure.
     */
    protected void onPutPolicyFailure(LifecyclePolicy policy, Exception e) {
        logger.error(() -> format("error adding lifecycle policy [%s] for [%s]", policy.getName(), getOrigin()), e);
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

        if (isClusterReady(event) == false) {
            return;
        }

        // if this node is newer than the master node, we probably need to add the template, which might be newer than the
        // template the master node has, so we need potentially add new templates despite being not the master node
        DiscoveryNode localNode = event.state().getNodes().getLocalNode();
        boolean localNodeVersionAfterMaster = localNode.getVersion().after(masterNode.getVersion());

        if (event.localNodeMaster() || localNodeVersionAfterMaster) {
            addIngestPipelinesIfMissing(state);
            addTemplatesIfMissing(state);
            addIndexLifecyclePoliciesIfMissing(state);
        }
    }

    /**
     * A method that can be overridden to add additional conditions to be satisfied
     * before installing the template registry components.
     */
    protected boolean isClusterReady(ClusterChangedEvent event) {
        return true;
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
        if (isDataStreamsLifecycleOnlyMode(clusterService.getSettings())) {
            // data stream lifecycle cannot be configured via legacy templates
            return;
        }
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
                    logger.info(
                        "upgrading legacy template [{}] for [{}] from version [{}] to version [{}]",
                        templateName,
                        getOrigin(),
                        currentTemplate.getVersion(),
                        newTemplate.getVersion()
                    );
                    putLegacyTemplate(newTemplate, creationCheck);
                } else {
                    creationCheck.set(false);
                    logger.trace(
                        "not adding legacy template [{}] for [{}], because it already exists at version [{}]",
                        templateName,
                        getOrigin(),
                        currentTemplate.getVersion()
                    );
                }
            } else {
                logger.trace(
                    "skipping the creation of legacy template [{}] for [{}], because its creation is in progress",
                    templateName,
                    getOrigin()
                );
            }
        }
    }

    private void addComponentTemplatesIfMissing(ClusterState state) {
        final Map<String, ComponentTemplate> indexTemplates = getComponentTemplateConfigs();
        for (Map.Entry<String, ComponentTemplate> newTemplate : indexTemplates.entrySet()) {
            final String templateName = newTemplate.getKey();
            final AtomicBoolean creationCheck = templateCreationsInProgress.computeIfAbsent(templateName, key -> new AtomicBoolean(false));
            if (creationCheck.compareAndSet(false, true)) {
                ComponentTemplate currentTemplate = state.metadata().componentTemplates().get(templateName);
                if (templateDependenciesSatisfied(state, newTemplate.getValue()) == false) {
                    creationCheck.set(false);
                    logger.trace(
                        "not adding index template [{}] for [{}] because its required dependencies do not exist",
                        templateName,
                        getOrigin()
                    );
                } else if (Objects.isNull(currentTemplate)) {
                    logger.debug("adding component template [{}] for [{}], because it doesn't exist", templateName, getOrigin());
                    putComponentTemplate(templateName, newTemplate.getValue(), creationCheck);
                } else if (Objects.isNull(currentTemplate.version()) || newTemplate.getValue().version() > currentTemplate.version()) {
                    // IndexTemplateConfig now enforces templates contain a `version` property, so if the template doesn't have one we can
                    // safely assume it's an old version of the template.
                    logger.info(
                        "upgrading component template [{}] for [{}] from version [{}] to version [{}]",
                        templateName,
                        getOrigin(),
                        currentTemplate.version(),
                        newTemplate.getValue().version()
                    );
                    putComponentTemplate(templateName, newTemplate.getValue(), creationCheck);
                } else {
                    creationCheck.set(false);
                    logger.trace(
                        "not adding component template [{}] for [{}], because it already exists at version [{}]",
                        templateName,
                        getOrigin(),
                        currentTemplate.version()
                    );
                }
            } else {
                logger.trace(
                    "skipping the creation of component template [{}] for [{}], because its creation is in progress",
                    templateName,
                    getOrigin()
                );
            }
        }
    }

    /**
     * Returns true if the cluster state contains all of the dependencies required by the provided component template
     */
    private static boolean templateDependenciesSatisfied(ClusterState state, ComponentTemplate indexTemplate) {
        Template template = indexTemplate.template();
        if (template == null) {
            return true;
        }
        Settings settings = template.settings();
        if (settings == null) {
            return true;
        }
        IngestMetadata ingestMetadata = state.metadata().custom(IngestMetadata.TYPE);
        String defaultPipeline = settings.get("index.default_pipeline");
        if (defaultPipeline != null) {
            if (ingestMetadata == null || ingestMetadata.getPipelines().containsKey(defaultPipeline) == false) {
                return false;
            }
        }
        String finalPipeline = settings.get("index.final_pipeline");
        if (finalPipeline != null) {
            return ingestMetadata != null && ingestMetadata.getPipelines().containsKey(finalPipeline);
        }
        return true;
    }

    private void addComposableTemplatesIfMissing(ClusterState state) {
        final Map<String, ComposableIndexTemplate> indexTemplates = getComposableTemplateConfigs();
        for (Map.Entry<String, ComposableIndexTemplate> newTemplate : indexTemplates.entrySet()) {
            final String templateName = newTemplate.getKey();
            final AtomicBoolean creationCheck = templateCreationsInProgress.computeIfAbsent(templateName, key -> new AtomicBoolean(false));
            if (creationCheck.compareAndSet(false, true)) {
                ComposableIndexTemplate currentTemplate = state.metadata().templatesV2().get(templateName);
                boolean componentTemplatesAvailable = componentTemplatesInstalled(state, newTemplate.getValue());
                if (componentTemplatesAvailable == false) {
                    creationCheck.set(false);
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "not adding composable template [{}] for [{}] because its required component templates do not exist or do not "
                                + "have the right version",
                            templateName,
                            getOrigin()
                        );
                    }
                } else if (Objects.isNull(currentTemplate)) {
                    logger.debug("adding composable template [{}] for [{}], because it doesn't exist", templateName, getOrigin());
                    putComposableTemplate(state, templateName, newTemplate.getValue(), creationCheck, false);
                } else if (Objects.isNull(currentTemplate.version()) || newTemplate.getValue().version() > currentTemplate.version()) {
                    // IndexTemplateConfig now enforces templates contain a `version` property, so if the template doesn't have one we can
                    // safely assume it's an old version of the template.
                    logger.info(
                        "upgrading composable template [{}] for [{}] from version [{}] to version [{}]",
                        templateName,
                        getOrigin(),
                        currentTemplate.version(),
                        newTemplate.getValue().version()
                    );
                    putComposableTemplate(state, templateName, newTemplate.getValue(), creationCheck, true);
                } else {
                    creationCheck.set(false);
                    logger.trace(
                        "not adding composable template [{}] for [{}], because it already exists at version [{}]",
                        templateName,
                        getOrigin(),
                        currentTemplate.version()
                    );
                }
            } else {
                logger.trace(
                    "skipping the creation of composable template [{}] for [{}], because its creation is in progress",
                    templateName,
                    getOrigin()
                );
            }
        }
    }

    /**
     * Returns true if the cluster state contains all of the component templates needed by the composable template. If this registry
     * requires automatic rollover after index template upgrades (see {@link #applyRolloverAfterTemplateV2Upgrade()}), this method also
     * verifies that the installed components templates are of the right version.
     */
    private boolean componentTemplatesInstalled(ClusterState state, ComposableIndexTemplate indexTemplate) {
        if (applyRolloverAfterTemplateV2Upgrade() == false) {
            // component templates and index templates can be updated independently, we only need to know that the required component
            // templates are available
            return state.metadata().componentTemplates().keySet().containsAll(indexTemplate.getRequiredComponentTemplates());
        }
        Map<String, ComponentTemplate> componentTemplateConfigs = getComponentTemplateConfigs();
        Map<String, ComponentTemplate> installedTemplates = state.metadata().componentTemplates();
        for (String templateName : indexTemplate.getRequiredComponentTemplates()) {
            ComponentTemplate installedTemplate = installedTemplates.get(templateName);
            // if a required component templates is not installed - the current cluster state cannot allow this index template yet
            if (installedTemplate == null) {
                return false;
            }
            ComponentTemplate templateConfig = componentTemplateConfigs.get(templateName);
            // note: currently we only take care of component templates that are installed by the same registry as the index template. We
            // don't enforce proper version for component templates that come from outside of this registry.
            // See https://github.com/elastic/elasticsearch/issues/99647
            if (templateConfig != null && templateConfig.version().equals(installedTemplate.version()) == false) {
                return false;
            }
        }
        return true;
    }

    private void putLegacyTemplate(final IndexTemplateConfig config, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            final String templateName = config.getTemplateName();

            PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName).source(config.loadBytes(), XContentType.JSON);
            request.masterNodeTimeout(TimeValue.MAX_VALUE);
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                getOrigin(),
                request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        creationCheck.set(false);
                        if (response.isAcknowledged() == false) {
                            logger.error(
                                "error adding legacy template [{}] for [{}], request was not acknowledged",
                                templateName,
                                getOrigin()
                            );
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        onPutTemplateFailure(templateName, e);
                    }
                },
                client.admin().indices()::putTemplate
            );
        });
    }

    private void putComponentTemplate(final String templateName, final ComponentTemplate template, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request(templateName).componentTemplate(template);
            request.masterNodeTimeout(TimeValue.MAX_VALUE);
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                getOrigin(),
                request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        creationCheck.set(false);
                        if (response.isAcknowledged() == false) {
                            logger.error(
                                "error adding component template [{}] for [{}], request was not acknowledged",
                                templateName,
                                getOrigin()
                            );
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        onPutTemplateFailure(templateName, e);
                    }
                },
                (req, listener) -> client.execute(PutComponentTemplateAction.INSTANCE, req, listener)
            );
        });
    }

    private void putComposableTemplate(
        ClusterState state,
        final String templateName,
        final ComposableIndexTemplate indexTemplate,
        final AtomicBoolean creationCheck,
        final boolean isUpgrade
    ) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(templateName)
                .indexTemplate(indexTemplate);
            request.masterNodeTimeout(TimeValue.MAX_VALUE);
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                getOrigin(),
                request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        if (response.isAcknowledged()) {
                            if (isUpgrade && applyRolloverAfterTemplateV2Upgrade()) {
                                invokeRollover(state, templateName, indexTemplate, creationCheck);
                            } else {
                                creationCheck.set(false);
                            }
                        } else {
                            creationCheck.set(false);
                            logger.error(
                                "error adding composable template [{}] for [{}], request was not acknowledged",
                                templateName,
                                getOrigin()
                            );
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        onPutTemplateFailure(templateName, e);
                    }
                },
                (req, listener) -> client.execute(TransportPutComposableIndexTemplateAction.TYPE, req, listener)
            );
        });
    }

    private void addIndexLifecyclePoliciesIfMissing(ClusterState state) {
        if (isDataStreamsLifecycleOnlyMode(clusterService.getSettings())) {
            logger.trace("running in data stream lifecycle only mode. skipping the installation of ILM policies.");
            return;
        }
        IndexLifecycleMetadata metadata = state.metadata().custom(IndexLifecycleMetadata.TYPE);
        for (LifecyclePolicy policy : getLifecyclePolicies()) {
            final AtomicBoolean creationCheck = policyCreationsInProgress.computeIfAbsent(
                policy.getName(),
                key -> new AtomicBoolean(false)
            );
            if (creationCheck.compareAndSet(false, true)) {
                final LifecyclePolicy currentPolicy = metadata != null ? metadata.getPolicies().get(policy.getName()) : null;
                if (Objects.isNull(currentPolicy)) {
                    logger.debug("adding lifecycle policy [{}] for [{}], because it doesn't exist", policy.getName(), getOrigin());
                    putPolicy(policy, creationCheck);
                } else if (isUpgradeRequired(currentPolicy, policy)) {
                    logger.info("upgrading lifecycle policy [{}] for [{}]", policy.getName(), getOrigin());
                    putPolicy(policy, creationCheck);
                } else {
                    logger.trace("not adding lifecycle policy [{}] for [{}], because it already exists", policy.getName(), getOrigin());
                    creationCheck.set(false);
                }
            }
        }
    }

    /**
     * Determines whether an index lifecycle policy should be upgraded to a newer version.
     *
     * @param currentPolicy The current lifecycle policy. Never null.
     * @param newPolicy The new lifecycle policy. Never null.
     * @return <code>true</code> if <code>newPolicy</code> should replace <code>currentPolicy</code>.
     */
    protected boolean isUpgradeRequired(LifecyclePolicy currentPolicy, LifecyclePolicy newPolicy) {
        return false;
    }

    private void putPolicy(final LifecyclePolicy policy, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            PutLifecycleRequest request = new PutLifecycleRequest(policy);
            request.masterNodeTimeout(TimeValue.MAX_VALUE);
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                getOrigin(),
                request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        creationCheck.set(false);
                        if (response.isAcknowledged() == false) {
                            logger.error(
                                "error adding lifecycle policy [{}] for [{}], request was not acknowledged",
                                policy.getName(),
                                getOrigin()
                            );
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        onPutPolicyFailure(policy, e);
                    }
                },
                (req, listener) -> client.execute(ILMActions.PUT, req, listener)
            );
        });
    }

    protected static Map<String, ComposableIndexTemplate> parseComposableTemplates(IndexTemplateConfig... config) {
        return Arrays.stream(config).collect(Collectors.toUnmodifiableMap(IndexTemplateConfig::getTemplateName, indexTemplateConfig -> {
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, indexTemplateConfig.loadBytes())) {
                return ComposableIndexTemplate.parse(parser);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }));
    }

    private void addIngestPipelinesIfMissing(ClusterState state) {
        for (IngestPipelineConfig requiredPipeline : getIngestPipelines()) {
            final AtomicBoolean creationCheck = pipelineCreationsInProgress.computeIfAbsent(
                requiredPipeline.getId(),
                key -> new AtomicBoolean(false)
            );

            if (creationCheck.compareAndSet(false, true)) {
                List<String> pipelineDependencies = requiredPipeline.getPipelineDependencies();
                if (pipelineDependencies != null && pipelineDependenciesExist(state, pipelineDependencies) == false) {
                    creationCheck.set(false);
                    logger.trace(
                        "not adding ingest pipeline [{}] for [{}] because its dependencies do not exist",
                        requiredPipeline.getId(),
                        getOrigin()
                    );
                } else {
                    PipelineConfiguration existingPipeline = findInstalledPipeline(state, requiredPipeline.getId());
                    if (existingPipeline != null) {
                        Integer existingPipelineVersion = existingPipeline.getVersion();
                        if (existingPipelineVersion == null || existingPipelineVersion < requiredPipeline.getVersion()) {
                            logger.info(
                                "upgrading ingest pipeline [{}] for [{}] from version [{}] to version [{}]",
                                requiredPipeline.getId(),
                                getOrigin(),
                                existingPipelineVersion,
                                requiredPipeline.getVersion()
                            );
                            putIngestPipeline(requiredPipeline, creationCheck);
                        } else {
                            creationCheck.set(false);
                            logger.debug(
                                "not adding ingest pipeline [{}] for [{}], because it already exists",
                                requiredPipeline.getId(),
                                getOrigin()
                            );
                        }
                    } else {
                        logger.debug(
                            "adding ingest pipeline [{}] for [{}], because it doesn't exist",
                            requiredPipeline.getId(),
                            getOrigin()
                        );
                        putIngestPipeline(requiredPipeline, creationCheck);
                    }
                }
            }
        }
    }

    private static boolean pipelineDependenciesExist(ClusterState state, List<String> dependencies) {
        for (String dependency : dependencies) {
            if (findInstalledPipeline(state, dependency) == null) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    private static PipelineConfiguration findInstalledPipeline(ClusterState state, String pipelineId) {
        Optional<IngestMetadata> maybeMeta = Optional.ofNullable(state.metadata().custom(IngestMetadata.TYPE));
        return maybeMeta.map(ingestMetadata -> ingestMetadata.getPipelines().get(pipelineId)).orElse(null);
    }

    private void putIngestPipeline(final IngestPipelineConfig pipelineConfig, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            PutPipelineRequest request = new PutPipelineRequest(
                pipelineConfig.getId(),
                pipelineConfig.loadConfig(),
                pipelineConfig.getXContentType()
            );
            request.masterNodeTimeout(TimeValue.MAX_VALUE);

            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                getOrigin(),
                request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        creationCheck.set(false);
                        if (response.isAcknowledged() == false) {
                            logger.error(
                                "error adding ingest pipeline [{}] for [{}], request was not acknowledged",
                                pipelineConfig.getId(),
                                getOrigin()
                            );
                        } else {
                            logger.info("adding ingest pipeline {}", pipelineConfig.getId());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        onPutPipelineFailure(pipelineConfig.getId(), e);
                    }
                },
                (req, listener) -> client.execute(PutPipelineTransportAction.TYPE, req, listener)
            );
        });
    }

    /**
     * Allows registries to opt-in for automatic rollover of "relevant" data streams immediately after a composable index template gets
     * upgraded. If set to {@code true}, then every time a composable index template is being upgraded, all data streams of which name
     * matches this template's index patterns AND of all matching templates the upgraded one has the highest priority, will be rolled over.
     *
     * @return {@code true} if this registry wants to apply automatic rollovers after template V2 upgrades
     */
    protected boolean applyRolloverAfterTemplateV2Upgrade() {
        return false;
    }

    /**
     * Called when creation of an ingest pipeline fails.
     *
     * @param pipelineId the pipeline that failed to be created.
     * @param e The exception that caused the failure.
     */
    protected void onPutPipelineFailure(String pipelineId, Exception e) {
        logger.error(() -> format("error adding ingest pipeline template [%s] for [%s]", pipelineId, getOrigin()), e);
    }

    private void invokeRollover(
        final ClusterState state,
        final String templateName,
        final ComposableIndexTemplate indexTemplate,
        final AtomicBoolean creationCheck
    ) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            List<String> rolloverTargets = findRolloverTargetDataStreams(state, templateName, indexTemplate);
            if (rolloverTargets.isEmpty() == false) {
                GroupedActionListener<RolloverResponse> groupedActionListener = new GroupedActionListener<>(
                    rolloverTargets.size(),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Collection<RolloverResponse> rolloverResponses) {
                            creationCheck.set(false);
                            onRolloversBulkResponse(rolloverResponses);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            creationCheck.set(false);
                            onRolloverFailure(e);
                        }
                    }
                );
                for (String rolloverTarget : rolloverTargets) {
                    logger.info(
                        "rolling over data stream [{}] lazily as a followup to the upgrade of the [{}] index template [{}]",
                        rolloverTarget,
                        getOrigin(),
                        templateName
                    );
                    RolloverRequest request = new RolloverRequest(rolloverTarget, null);
                    request.lazy(true);
                    request.masterNodeTimeout(TimeValue.MAX_VALUE);
                    executeAsyncWithOrigin(
                        client.threadPool().getThreadContext(),
                        getOrigin(),
                        request,
                        groupedActionListener,
                        (req, listener) -> client.execute(RolloverAction.INSTANCE, req, listener)
                    );
                }
            }
        });
    }

    void onRolloversBulkResponse(Collection<RolloverResponse> rolloverResponses) {
        for (RolloverResponse rolloverResponse : rolloverResponses) {
            if (rolloverResponse.isRolledOver() == false) {
                logger.warn("rollover of the [{}] index [{}] failed", getOrigin(), rolloverResponse.getOldIndex());
            }
        }
    }

    void onRolloverFailure(Exception e) {
        logger.error(String.format(Locale.ROOT, "[%s] related rollover failed", getOrigin()), e);
        for (Throwable throwable : e.getSuppressed()) {
            logger.error(String.format(Locale.ROOT, "[%s] related rollover failed", getOrigin()), throwable);
        }
    }

    /**
     * Finds rollover target data streams based on the provided index template. A matching rollover target data stream is such that:
     * <ol>
     *     <li> matches the provided index template's index patterns
     *     <li> of all index templates that have index patterns matching its name, the one with the highest priority is the one provided
     *     as argument
     * </ol>
     *
     * @param state         the cluster state
     * @param templateName  the ID by which the provided index template is being registered
     * @param indexTemplate the index template for which a data stream is looked up as rollover target
     * @return the list of rollover targets matching the provided index template
     */
    static List<String> findRolloverTargetDataStreams(ClusterState state, String templateName, ComposableIndexTemplate indexTemplate) {
        final Metadata metadata = state.metadata();
        return metadata.dataStreams()
            .values()
            .stream()
            // Limit to checking data streams that match any of the index template's index patterns
            .filter(ds -> indexTemplate.indexPatterns().stream().anyMatch(pattern -> Regex.simpleMatch(pattern, ds.getName())))
            .filter(ds -> templateName.equals(MetadataIndexTemplateService.findV2Template(metadata, ds.getName(), ds.isHidden())))
            .map(DataStream::getName)
            .collect(Collectors.toList());
    }
}
