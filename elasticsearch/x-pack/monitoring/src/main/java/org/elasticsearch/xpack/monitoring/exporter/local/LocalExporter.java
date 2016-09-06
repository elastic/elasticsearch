/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.local;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.xpack.monitoring.exporter.ExportBulk;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.xpack.monitoring.resolver.ResolversRegistry;
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;
import org.elasticsearch.xpack.security.InternalClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;

/**
 *
 */
public class LocalExporter extends Exporter implements ClusterStateListener, CleanerService.Listener {

    private static final Logger logger = Loggers.getLogger(LocalExporter.class);

    public static final String TYPE = "local";

    private final InternalClient client;
    private final ClusterService clusterService;
    private final ResolversRegistry resolvers;
    private final CleanerService cleanerService;

    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);

    public LocalExporter(Exporter.Config config, InternalClient client,
                         ClusterService clusterService, CleanerService cleanerService) {
        super(config);
        this.client = client;
        this.clusterService = clusterService;
        this.cleanerService = cleanerService;
        this.resolvers = new ResolversRegistry(config.settings());

        // Checks that required templates are loaded
        for (MonitoringIndexNameResolver resolver : resolvers) {
            if (resolver.template() == null) {
                throw new IllegalStateException("unable to find built-in template " + resolver.templateName());
            }
        }

        clusterService.add(this);
        cleanerService.add(this);
    }

    ResolversRegistry getResolvers() {
        return resolvers;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (state.get() == State.INITIALIZED) {
            resolveBulk(event.state());
        }
    }

    @Override
    public ExportBulk openBulk() {
        if (state.get() !=  State.RUNNING) {
            return null;
        }
        return resolveBulk(clusterService.state());
    }

    @Override
    public void doClose() {
        if (state.getAndSet(State.TERMINATED) != State.TERMINATED) {
            logger.trace("stopped");
            clusterService.remove(this);
            cleanerService.remove(this);
        }
    }

    LocalBulk resolveBulk(ClusterState clusterState) {
        if (clusterService.localNode() == null || clusterState == null) {
            return null;
        }

        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .monitoring-es-
            // indices but they may not have been restored from the cluster state on disk
            logger.debug("waiting until gateway has recovered from disk");
            return null;
        }

        // List of distinct templates
        Map<String, String> templates = StreamSupport.stream(new ResolversRegistry(Settings.EMPTY).spliterator(), false)
                .collect(Collectors.toMap(MonitoringIndexNameResolver::templateName, MonitoringIndexNameResolver::template, (a, b) -> a));


        // if this is not the master, we'll just look to see if the monitoring templates are installed.
        // If they all are, we'll be able to start this exporter. Otherwise, we'll just wait for a new cluster state.
        if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
            for (String template : templates.keySet()) {
                if (hasTemplate(template, clusterState) == false) {
                    // the required template is not yet installed in the given cluster state, we'll wait.
                    logger.debug("monitoring index template [{}] does not exist, so service cannot start", template);
                    return null;
                }
            }

            // if we don't have the ingest pipeline, then it's going to fail anyway
            if (hasIngestPipelines(clusterState) == false) {
                logger.debug("monitoring ingest pipeline [{}] does not exist, so service cannot start", EXPORT_PIPELINE_NAME);
                return null;
            }

            logger.trace("monitoring index templates and pipelines are installed, service can start");

        } else {

            // we are on the elected master
            //
            // Check that there is nothing that could block metadata updates
            if (clusterState.blocks().hasGlobalBlock(ClusterBlockLevel.METADATA_WRITE)) {
                logger.debug("waiting until metadata writes are unblocked");
                return null;
            }

            // whenever we install anything, we return null to force it to retry to give the cluster a chance to catch up
            boolean installedSomething = false;

            // Check that each required template exist, installing it if needed
            for (Map.Entry<String, String> template : templates.entrySet()) {
                if (hasTemplate(template.getKey(), clusterState) == false) {
                    logger.debug("template [{}] not found", template.getKey());
                    putTemplate(template.getKey(), template.getValue());
                    installedSomething = true;
                } else {
                    logger.trace("template [{}] found", template.getKey());
                }
            }

            // if we don't have the ingest pipeline, then install it
            if (hasIngestPipelines(clusterState) == false) {
                logger.debug("pipeline [{}] not found", EXPORT_PIPELINE_NAME);
                putIngestPipeline();
                installedSomething = true;
            } else {
                logger.trace("pipeline [{}] found", EXPORT_PIPELINE_NAME);
            }

            if (installedSomething) {
                // let the cluster catch up (and because we do the PUTs asynchronously)
                return null;
            }

            logger.trace("monitoring index templates and pipelines are installed on master node, service can start");
        }

        if (state.compareAndSet(State.INITIALIZED, State.RUNNING)) {
            logger.debug("started");
        }

        return new LocalBulk(name(), logger, client, resolvers, config.settings().getAsBoolean(USE_INGEST_PIPELINE_SETTING, true));
    }

    /**
     * Determine if the ingest pipeline for {@link #EXPORT_PIPELINE_NAME} exists in the cluster or not.
     *
     * @param clusterState The current cluster state
     * @return {@code true} if the {@code clusterState} contains a pipeline with {@link #EXPORT_PIPELINE_NAME}
     */
    private boolean hasIngestPipelines(ClusterState clusterState) {
        final IngestMetadata ingestMetadata = clusterState.getMetaData().custom(IngestMetadata.TYPE);

        // NOTE: this will need to become a more advanced check once we actually supply a meaningful pipeline
        //  because we will want to _replace_ older pipelines so that they go from (e.g., monitoring-2 doing nothing to
        //  monitoring-2 becoming monitoring-3 documents)
        return ingestMetadata != null && ingestMetadata.getPipelines().containsKey(EXPORT_PIPELINE_NAME);
    }

    /**
     * Create the pipeline required to handle past data as well as to future-proof ingestion for <em>current</em> documents (the pipeline
     * is initially empty, but it can be replaced later with one that translates it as-needed).
     * <p>
     * This should only be invoked by the <em>elected</em> master node.
     * <p>
     * Whenever we eventually make a backwards incompatible change, then we need to override any pipeline that already exists that is
     * older than this one. Currently, we prepend the API version as the first part of the description followed by a <code>:</code>.
     * <pre><code>
     * {
     *   "description": "2: This is a placeholder ...",
     *   "pipelines" : [ ... ]
     * }
     * </code></pre>
     * That should be used (until something better exists) to ensure that we do not override <em>newer</em> pipelines with our own.
     */
    private void putIngestPipeline() {
        logger.debug("installing ingest pipeline [{}]", EXPORT_PIPELINE_NAME);

        final BytesReference emptyPipeline = emptyPipeline(XContentType.JSON).bytes();
        final PutPipelineRequest request = new PutPipelineRequest(EXPORT_PIPELINE_NAME, emptyPipeline);

        client.admin().cluster().putPipeline(request, new ResponseActionListener<>("pipeline", EXPORT_PIPELINE_NAME));
    }

    /**
     * List templates that exists in cluster state metadata and that match a given template name pattern.
     */
    private ImmutableOpenMap<String, Integer> findTemplates(String templatePattern, ClusterState state) {
        if (state == null || state.getMetaData() == null || state.getMetaData().getTemplates().isEmpty()) {
            return ImmutableOpenMap.of();
        }

        ImmutableOpenMap.Builder<String, Integer> templates = ImmutableOpenMap.builder();
        for (ObjectCursor<String> template : state.metaData().templates().keys()) {
            if (Regex.simpleMatch(templatePattern, template.value)) {
                try {
                    Integer version = Integer.parseInt(template.value.substring(templatePattern.length() - 1));
                    templates.put(template.value, version);
                    logger.debug("found index template [{}] in version [{}]", template.value, version);
                } catch (NumberFormatException e) {
                    logger.warn("cannot extract version number for template [{}]", template.value);
                }
            }
        }
        return templates.build();
    }

    private boolean hasTemplate(String templateName, ClusterState state) {
        ImmutableOpenMap<String, Integer> templates = findTemplates(templateName, state);
        return templates.size() > 0;
    }

    private void putTemplate(String template, String source) {
        logger.debug("installing template [{}]",template);

        PutIndexTemplateRequest request = new PutIndexTemplateRequest(template).source(source);
        assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";

        // async call, so we won't block cluster event thread
        client.admin().indices().putTemplate(request, new ResponseActionListener<>("template", template));
    }

    @Override
    public void onCleanUpIndices(TimeValue retention) {
        if (state.get() != State.RUNNING) {
            logger.debug("exporter not ready");
            return;
        }

        if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
            // Reference date time will be compared to index.creation_date settings,
            // that's why it must be in UTC
            DateTime expiration = new DateTime(DateTimeZone.UTC).minus(retention.millis());
            logger.debug("cleaning indices [expiration={}, retention={}]", expiration, retention);

            ClusterState clusterState = clusterService.state();
            if (clusterState != null) {
                long expirationTime = expiration.getMillis();

                // Get the list of monitoring index patterns
                String[] patterns = StreamSupport.stream(getResolvers().spliterator(), false)
                                                .map(MonitoringIndexNameResolver::indexPattern)
                                                .distinct()
                                                .toArray(String[]::new);

                MonitoringDoc monitoringDoc = new MonitoringDoc(null, null);
                monitoringDoc.setTimestamp(System.currentTimeMillis());

                // Get the names of the current monitoring indices
                Set<String> currents = StreamSupport.stream(getResolvers().spliterator(), false)
                                                    .map(r -> r.index(monitoringDoc))
                                                    .collect(Collectors.toSet());

                Set<String> indices = new HashSet<>();
                for (ObjectObjectCursor<String, IndexMetaData> index : clusterState.getMetaData().indices()) {
                    String indexName =  index.key;

                    if (Regex.simpleMatch(patterns, indexName)) {

                        // Never delete the data index or a current index
                        if (currents.contains(indexName)) {
                            continue;
                        }

                        long creationDate = index.value.getCreationDate();
                        if (creationDate <= expirationTime) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("detected expired index [name={}, created={}, expired={}]",
                                        indexName, new DateTime(creationDate, DateTimeZone.UTC), expiration);
                            }
                            indices.add(indexName);
                        }
                    }
                }

                if (!indices.isEmpty()) {
                    logger.info("cleaning up [{}] old indices", indices.size());
                    deleteIndices(indices);
                } else {
                    logger.debug("no old indices found for clean up");
                }
            }
        }
    }

    private void deleteIndices(Set<String> indices) {
        logger.trace("deleting {} indices: [{}]", indices.size(), collectionToCommaDelimitedString(indices));
        client.admin().indices().delete(new DeleteIndexRequest(indices.toArray(new String[indices.size()])),
                new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse response) {
                if (response.isAcknowledged()) {
                    logger.debug("{} indices deleted", indices.size());
                } else {
                    // Probably means that the delete request has timed out,
                    // the indices will survive until the next clean up.
                    logger.warn("deletion of {} indices wasn't acknowledged", indices.size());
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("failed to delete indices", e);
            }
        });
    }

    enum State {
        INITIALIZED,
        RUNNING,
        TERMINATED
    }

    /**
     * Acknowledge success / failure for any given creation attempt (e.g., template or pipeline).
     */
    private class ResponseActionListener<Response extends AcknowledgedResponse> implements ActionListener<Response> {

        private final String type;
        private final String name;

        public ResponseActionListener(String type, String name) {
            this.type = Objects.requireNonNull(type);
            this.name = Objects.requireNonNull(name);
        }

        @Override
        public void onResponse(Response response) {
            if (response.isAcknowledged()) {
                logger.trace("successfully set monitoring {} [{}]", type, name);
            } else {
                logger.error("failed to set monitoring index {} [{}]", type, name);
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to set monitoring index {} [{}]", type, name), e);
        }
    }
}
