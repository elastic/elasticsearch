/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ingest.PipelineConfigurationTemplate;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class AnalyticsTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(AnalyticsTemplateRegistry.class);
    public static final String ROOT_RESOURCE_PATH = "/org/elasticsearch/xpack/entsearch/analytics/";

    // This number must be incremented when we make changes to built-in templates.
    public static final int REGISTRY_VERSION = 1;

    // The variable to be replaced with the template version number
    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.entsearch.analytics.template.version";

    // ILM Policies configuration
    public static final String EVENT_DATA_STREAM_ILM_POLICY_NAME = "behavioral_analytics-events-default_policy";
    private static final List<LifecyclePolicy> LIFECYCLE_POLICIES = Stream.of(
        new LifecyclePolicyConfig(EVENT_DATA_STREAM_ILM_POLICY_NAME, ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_ILM_POLICY_NAME + ".json")
    ).map(config -> config.load(LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY)).toList();

    // Index template components configuration
    public static final String EVENT_DATA_STREAM_SETTINGS_COMPONENT_NAME = "behavioral_analytics-events-settings";
    public static final String EVENT_DATA_STREAM_MAPPINGS_COMPONENT_NAME = "behavioral_analytics-events-mappings";

    private static final Map<String, ComponentTemplate> COMPONENT_TEMPLATES;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                EVENT_DATA_STREAM_SETTINGS_COMPONENT_NAME,
                ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_SETTINGS_COMPONENT_NAME + ".json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                EVENT_DATA_STREAM_MAPPINGS_COMPONENT_NAME,
                ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_MAPPINGS_COMPONENT_NAME + ".json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            )
        )) {
            try {
                componentTemplates.put(
                    config.getTemplateName(),
                    ComponentTemplate.parse(JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes()))
                );
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        COMPONENT_TEMPLATES = Map.copyOf(componentTemplates);
    }

    // Composable index templates configuration.
    public static final String EVENT_DATA_STREAM_INDEX_PREFIX = "behavioral_analytics-events-";

    public static final String EVENT_DATA_STREAM_INDEX_PATTERN = EVENT_DATA_STREAM_INDEX_PREFIX + "*";
    public static final String EVENT_DATA_STREAM_TEMPLATE_NAME = "behavioral_analytics-events-default";

    private static final String EVENT_DATA_STREAM_TEMPLATE_FILENAME = "behavioral_analytics-events-template";

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATES = parseComposableTemplates(
        new IndexTemplateConfig(
            EVENT_DATA_STREAM_TEMPLATE_NAME,
            ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_TEMPLATE_FILENAME + ".json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            Map.of("event_data_stream.index_pattern", EVENT_DATA_STREAM_INDEX_PATTERN)
        )
    );

    // Ingest pipeline configuration.
    protected final ConcurrentMap<String, AtomicBoolean> pipelineCreationsInProgress = new ConcurrentHashMap<>();

    public static final String EVENT_DATA_STREAM_PIPELINE_NAME = "behavioral_analytics-events-final_pipeline";
    private static final List<PipelineConfigurationTemplate> INGEST_PIPELINES = Collections.singletonList(
        new PipelineConfigurationTemplate(
            EVENT_DATA_STREAM_PIPELINE_NAME,
            ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_PIPELINE_NAME + ".json",
            Map.of(TEMPLATE_VERSION_VARIABLE, String.valueOf(REGISTRY_VERSION))
        )
    );

    public AnalyticsTemplateRegistry(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(Settings.EMPTY, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        super.clusterChanged(event);

        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        // no master node, exit immediately
        DiscoveryNode masterNode = event.state().getNodes().getMasterNode();
        if (masterNode == null) {
            return;
        }

        if (requiresMasterNode() && state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        DiscoveryNode localNode = event.state().getNodes().getLocalNode();
        boolean localNodeVersionAfterMaster = localNode.getVersion().after(masterNode.getVersion());

        if (event.localNodeMaster() || localNodeVersionAfterMaster) {
            addIngestPipelineIfMissing(event.state());
        }
    }

    @Override
    protected String getOrigin() {
        return ENT_SEARCH_ORIGIN;
    }

    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        return LIFECYCLE_POLICIES;
    }

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        return COMPONENT_TEMPLATES;
    }

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return COMPOSABLE_INDEX_TEMPLATES;
    }

    protected List<PipelineConfigurationTemplate> getIngestPipelineConfigs() {
        return INGEST_PIPELINES;
    }

    @Override
    protected boolean requiresMasterNode() {
        // We are using the composable index template and component APIs,
        // these APIs aren't supported in 7.7 and earlier and in mixed cluster
        // environments this can cause a lot of ActionNotFoundTransportException
        // errors in the logs during rolling upgrades. If these templates
        // are only installed via elected master node then the APIs are always
        // there and the ActionNotFoundTransportException errors are then prevented.
        return true;
    }

    private void addIngestPipelineIfMissing(ClusterState state) {
        for (PipelineConfigurationTemplate pipelineConfigurationTemplate : getIngestPipelineConfigs()) {
            final AtomicBoolean creationCheck = pipelineCreationsInProgress.computeIfAbsent(
                pipelineConfigurationTemplate.id(),
                key -> new AtomicBoolean(false)
            );
            try {
                if (creationCheck.compareAndSet(false, true)) {
                    if (ingestPipelineExists(state, pipelineConfigurationTemplate.id())) {
                        IngestMetadata ingestMetadata = state.metadata().custom(IngestMetadata.TYPE);
                        PipelineConfiguration existingPipeline = ingestMetadata.getPipelines().get(pipelineConfigurationTemplate.id());
                        PipelineConfiguration newPipeline = pipelineConfigurationTemplate.loadPipelineConfiguration();

                        if (Objects.isNull(existingPipeline.getVersion()) || existingPipeline.getVersion() < newPipeline.getVersion()) {
                            logger.info(
                                "upgrading ingest pipeline [{}] for [{}] from version [{}] to version [{}]",
                                pipelineConfigurationTemplate.id(),
                                getOrigin(),
                                existingPipeline.getVersion(),
                                newPipeline.getVersion()
                            );
                            putIngestPipeline(pipelineConfigurationTemplate, creationCheck);
                        } else {
                            logger.debug(
                                "not adding ingest pipeline [{}] for [{}], because it already exists",
                                pipelineConfigurationTemplate.id(),
                                getOrigin()
                            );
                            creationCheck.set(false);
                        }
                    } else {
                        logger.debug(
                            "adding ingest pipeline [{}] for [{}], because it doesn't exist",
                            pipelineConfigurationTemplate.id(),
                            getOrigin()
                        );
                        putIngestPipeline(pipelineConfigurationTemplate, creationCheck);
                    }
                }
            } catch (IOException e) {
                creationCheck.set(false);
                logger.error(
                    Strings.format(
                        "not adding ingest pipeline [{}] for [{}], because of an error when reading the config",
                        pipelineConfigurationTemplate.id(),
                        getOrigin()
                    ),
                    e
                );
            }
        }
    }

    private static boolean ingestPipelineExists(ClusterState state, String pipelineId) {
        Optional<IngestMetadata> maybeMeta = Optional.ofNullable(state.metadata().custom(IngestMetadata.TYPE));
        return maybeMeta.isPresent() && maybeMeta.get().getPipelines().containsKey(pipelineId);
    }

    private void putIngestPipeline(final PipelineConfigurationTemplate pipelineConfiguration, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            try {
                PutPipelineRequest request = new PutPipelineRequest(
                    pipelineConfiguration.id(),
                    pipelineConfiguration.parsedSource(),
                    pipelineConfiguration.xContentType()
                );

                request.masterNodeTimeout(TimeValue.timeValueMinutes(1));

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
                                    pipelineConfiguration.id(),
                                    getOrigin()
                                );
                            } else {
                                logger.info("adding ingest pipeline {}", pipelineConfiguration.id());
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            creationCheck.set(false);
                            onPutPipelineFailure(pipelineConfiguration.id(), e);
                        }
                    },
                    (req, listener) -> client.execute(PutPipelineAction.INSTANCE, req, listener)
                );

            } catch (IOException e) {
                creationCheck.set(false);
                logger.error(
                    Strings.format(
                        "not adding ingest pipeline [{}] for [{}], because of an error when reading the config",
                        pipelineConfiguration.id(),
                        getOrigin()
                    ),
                    e
                );
            }
        });
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
}
