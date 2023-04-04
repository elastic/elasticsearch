/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.utils.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public abstract class PipelineRegistry implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(PipelineRegistry.class);
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Client client;

    private final ConcurrentMap<String, AtomicBoolean> pipelineCreationsInProgress = new ConcurrentHashMap<>();

    public PipelineRegistry(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
    }

    /**
     * Initialize the template registry, adding it as a listener so templates will be installed as necessary
     */
    public void initialize() {
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

        // This registry requires to run on a master node.
        // If not a master node, exit.
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        // if this node is newer than the master node, we probably need to add the template, which might be newer than the
        // template the master node has, so we need potentially add new templates despite being not the master node
        DiscoveryNode localNode = event.state().getNodes().getLocalNode();
        boolean localNodeVersionAfterMaster = localNode.getVersion().after(masterNode.getVersion());

        if (event.localNodeMaster() || localNodeVersionAfterMaster) {
            addIngestPipelinesIfMissing(state);
        }
    }

    protected abstract String getOrigin();

    protected abstract List<PipelineTemplateConfiguration> getIngestPipelineConfigs();

    private void addIngestPipelinesIfMissing(ClusterState state) {
        for (PipelineTemplateConfiguration pipelineTemplateConfig : getIngestPipelineConfigs()) {
            PipelineConfiguration newPipeline = pipelineTemplateConfig.load();
            final AtomicBoolean creationCheck = pipelineCreationsInProgress.computeIfAbsent(
                newPipeline.getId(),
                key -> new AtomicBoolean(false)
            );

            if (creationCheck.compareAndSet(false, true)) {
                if (ingestPipelineExists(state, newPipeline.getId())) {
                    IngestMetadata ingestMetadata = state.metadata().custom(IngestMetadata.TYPE);
                    PipelineConfiguration existingPipeline = ingestMetadata.getPipelines().get(newPipeline.getId());
                    boolean newPipelineHasVersion = Objects.isNull(newPipeline.getVersion()) == false;
                    boolean oldPipelineHasVersion = Objects.isNull(existingPipeline.getVersion()) == false;
                    if (newPipelineHasVersion
                        && (oldPipelineHasVersion == false || existingPipeline.getVersion() < newPipeline.getVersion())) {
                        logger.info(
                            "upgrading ingest pipeline [{}] for [{}] from version [{}] to version [{}]",
                            newPipeline.getId(),
                            getOrigin(),
                            existingPipeline.getVersion(),
                            newPipeline.getVersion()
                        );
                        putIngestPipeline(newPipeline, creationCheck);
                    } else {
                        logger.debug(
                            "not adding ingest pipeline [{}] for [{}], because it already exists",
                            newPipeline.getId(),
                            getOrigin()
                        );
                        creationCheck.set(false);
                    }
                } else {
                    logger.debug("adding ingest pipeline [{}] for [{}], because it doesn't exist", newPipeline.getId(), getOrigin());
                    putIngestPipeline(newPipeline, creationCheck);
                }
            }
        }
    }

    private static boolean ingestPipelineExists(ClusterState state, String pipelineId) {
        Optional<IngestMetadata> maybeMeta = Optional.ofNullable(state.metadata().custom(IngestMetadata.TYPE));
        return maybeMeta.isPresent() && maybeMeta.get().getPipelines().containsKey(pipelineId);
    }

    private void putIngestPipeline(final PipelineConfiguration pipelineConfig, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            try {
                executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    getOrigin(),
                    createPutPipelineRequest(pipelineConfig),
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
                    (req, listener) -> client.execute(PutPipelineAction.INSTANCE, req, listener)
                );

            } catch (IOException e) {
                creationCheck.set(false);
                logger.error(
                    Strings.format(
                        "not adding ingest pipeline [{}] for [{}], because of an error when reading the config",
                        pipelineConfig.getId(),
                        getOrigin()
                    ),
                    e
                );
            }
        });
    }

    private PutPipelineRequest createPutPipelineRequest(PipelineConfiguration pipelineConfiguration) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            PutPipelineRequest request = new PutPipelineRequest(
                pipelineConfiguration.getId(),
                BytesReference.bytes(builder.map(pipelineConfiguration.getConfigAsMap())),
                builder.contentType()
            );

            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            return request;
        }
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
