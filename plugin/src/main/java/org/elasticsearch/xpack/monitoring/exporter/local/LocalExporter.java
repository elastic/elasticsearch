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
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.WritePipelineResponse;
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
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;
import org.elasticsearch.xpack.monitoring.exporter.ExportBulk;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.xpack.monitoring.resolver.ResolversRegistry;
import org.elasticsearch.xpack.security.InternalClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;

public class LocalExporter extends Exporter implements ClusterStateListener, CleanerService.Listener {

    private static final Logger logger = Loggers.getLogger(LocalExporter.class);

    public static final String TYPE = "local";

    private final InternalClient client;
    private final ClusterService clusterService;
    private final ResolversRegistry resolvers;
    private final CleanerService cleanerService;

    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);
    private final AtomicBoolean installingSomething = new AtomicBoolean(false);
    private final AtomicBoolean waitedForSetup = new AtomicBoolean(false);

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

        clusterService.addListener(this);
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
            // we also remove the listener in resolveBulk after we get to RUNNING, but it's okay to double-remove
            clusterService.removeListener(this);
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
        final Map<String, String> templates = StreamSupport.stream(new ResolversRegistry(Settings.EMPTY).spliterator(), false)
                .collect(Collectors.toMap(MonitoringIndexNameResolver::templateName, MonitoringIndexNameResolver::template, (a, b) -> a));

        boolean setup = true;

        // elected master node needs to setup templates; non-master nodes need to wait for it to be setup
        if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
            setup = setupIfElectedMaster(clusterState, templates);
        } else if (setupIfNotElectedMaster(clusterState, templates.keySet()) == false) {
            // the first pass will be false so that we don't bother users if the master took one-go to setup
            if (waitedForSetup.getAndSet(true)) {
                logger.info("waiting for elected master node [{}] to setup local exporter [{}] (does it have x-pack installed?)",
                            clusterService.state().nodes().getMasterNode(), config.name());
            }

            setup = false;
        }

        // any failure/delay to setup the local exporter stops it until the next pass (10s by default)
        if (setup == false) {
            return null;
        }

        if (state.compareAndSet(State.INITIALIZED, State.RUNNING)) {
            logger.debug("started");

            // we no longer need to receive cluster state updates
            clusterService.removeListener(this);
        }

        return new LocalBulk(name(), logger, client, resolvers, config.settings().getAsBoolean(USE_INGEST_PIPELINE_SETTING, true));
    }

    /**
     * When not on the elected master, we require all resources (mapping types, templates, and pipelines) to be available before we
     * attempt to run the exporter. If those resources do not exist, then it means the elected master's exporter has not yet run, so the
     * monitoring cluster (this one, as the local exporter) is not setup yet.
     *
     * @param clusterState The current cluster state.
     * @param templates All template names that should exist.
     * @return {@code true} indicates that all resources are available and the exporter can be used. {@code false} to stop and wait.
     */
    private boolean setupIfNotElectedMaster(final ClusterState clusterState, final Set<String> templates) {
        for (final String type : MonitoringTemplateUtils.NEW_DATA_TYPES) {
            if (hasMappingType(type, clusterState) == false) {
                // the required type is not yet there in the given cluster state, we'll wait.
                logger.debug("monitoring index mapping [{}] does not exist in [{}], so service cannot start",
                        type, MonitoringTemplateUtils.DATA_INDEX);
                return false;
            }
        }

        for (final String template : templates) {
            if (hasTemplate(template, clusterState) == false) {
                // the required template is not yet installed in the given cluster state, we'll wait.
                logger.debug("monitoring index template [{}] does not exist, so service cannot start", template);
                return false;
            }
        }

        // if we don't have the ingest pipeline, then it's going to fail anyway
        if (hasIngestPipelines(clusterState) == false) {
            logger.debug("monitoring ingest pipeline [{}] does not exist, so service cannot start", EXPORT_PIPELINE_NAME);
            return false;
        }

        if (null != prepareAddAliasesTo2xIndices(clusterState)) {
            logger.debug("old monitoring indexes exist without aliases, waiting for them to get new aliases");
            return false;
        }

        logger.trace("monitoring index templates and pipelines are installed, service can start");

        // everything is setup
        return true;
    }

    /**
     * When on the elected master, we setup all resources (mapping types, templates, and pipelines) before we attempt to run the exporter.
     * If those resources do not exist, then we will create them.
     *
     * @param clusterState The current cluster state.
     * @param templates All template names that should exist.
     * @return {@code true} indicates that all resources are "ready" and the exporter can be used. {@code false} to stop and wait.
     */
    private boolean setupIfElectedMaster(final ClusterState clusterState, final Map<String, String> templates) {
        // we are on the elected master
        // Check that there is nothing that could block metadata updates
        if (clusterState.blocks().hasGlobalBlock(ClusterBlockLevel.METADATA_WRITE)) {
            logger.debug("waiting until metadata writes are unblocked");
            return false;
        }

        if (installingSomething.get() == true) {
            logger.trace("already installing something, waiting for install to complete");
            return false;
        }

        // build a list of runnables for everything that is missing, but do not start execution
        final List<Runnable> asyncActions = new ArrayList<>();
        final AtomicInteger pendingResponses = new AtomicInteger(0);

        // Check that all necessary types exist for _xpack/monitoring/_bulk usage
        final List<String> missingMappingTypes = Arrays.stream(MonitoringTemplateUtils.NEW_DATA_TYPES)
                .filter((type) -> hasMappingType(type, clusterState) == false)
                .collect(Collectors.toList());

        // Check that each required template exist, installing it if needed
        final List<Entry<String, String>> missingTemplates = templates.entrySet()
                .stream()
                .filter((e) -> hasTemplate(e.getKey(), clusterState) == false)
                .collect(Collectors.toList());

        if (missingMappingTypes.isEmpty() == false) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("type {} not found",
                    missingMappingTypes.stream().collect(Collectors.toList())));
            for (final String type : missingMappingTypes) {
                asyncActions.add(() -> putMappingType(type, new ResponseActionListener<>("type", type, pendingResponses)));
            }
        }

        if (missingTemplates.isEmpty() == false) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("template {} not found",
                    missingTemplates.stream().map(Map.Entry::getKey).collect(Collectors.toList())));
            for (Entry<String, String> template : missingTemplates) {
                asyncActions.add(() -> putTemplate(template.getKey(), template.getValue(),
                        new ResponseActionListener<>("template", template.getKey(), pendingResponses)));
            }
        }

        // if we don't have the ingest pipeline, then install it
        if (hasIngestPipelines(clusterState) == false) {
            logger.debug("pipeline [{}] not found", EXPORT_PIPELINE_NAME);
            asyncActions.add(() -> putIngestPipeline(new ResponseActionListener<>("pipeline", EXPORT_PIPELINE_NAME, pendingResponses)));
        } else {
            logger.trace("pipeline [{}] found", EXPORT_PIPELINE_NAME);
        }

        IndicesAliasesRequest addAliasesTo2xIndices = prepareAddAliasesTo2xIndices(clusterState);
        if (addAliasesTo2xIndices == null) {
            logger.trace("there are no 2.x monitoring indices or they have all the aliases they need");
        } else {
            final List<String> monitoringIndices2x =  addAliasesTo2xIndices.getAliasActions().stream()
                    .flatMap((a) -> Arrays.stream(a.indices()))
                    .collect(Collectors.toList());
            logger.debug("there are 2.x monitoring indices {} and they are missing some aliases to make them compatible with 5.x",
                    monitoringIndices2x);
            asyncActions.add(() -> client.execute(IndicesAliasesAction.INSTANCE, addAliasesTo2xIndices,
                    new ActionListener<IndicesAliasesResponse>() {
                        @Override
                        public void onResponse(IndicesAliasesResponse response) {
                            responseReceived();
                            if (response.isAcknowledged()) {
                                logger.info("Added modern aliases to 2.x monitoring indices {}", monitoringIndices2x);
                            } else {
                                logger.info("Unable to add modern aliases to 2.x monitoring indices {}, response not acknowledged.",
                                        monitoringIndices2x);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            responseReceived();
                            logger.error((Supplier<?>)
                                    () -> new ParameterizedMessage("Unable to add modern aliases to 2.x monitoring indices {}",
                                            monitoringIndices2x), e);
                        }

                        private void responseReceived() {
                            if (pendingResponses.decrementAndGet() <= 0) {
                                logger.trace("all installation requests returned a response");
                                if (installingSomething.compareAndSet(true, false) == false) {
                                    throw new IllegalStateException("could not reset installing flag to false");
                                }
                            }
                        }
                    }));
        }

        if (asyncActions.size() > 0) {
            if (installingSomething.compareAndSet(false, true)) {
                pendingResponses.set(asyncActions.size());
                asyncActions.forEach(Runnable::run);
            } else {
                // let the cluster catch up since requested installations may be ongoing
                return false;
            }
        } else {
            logger.debug("monitoring index templates and pipelines are installed on master node, service can start");
        }

        // everything is setup (or running)
        return true;
    }

    /**
     * Determine if the mapping {@code type} exists in the {@linkplain MonitoringTemplateUtils#DATA_INDEX data index}.
     *
     * @param type The data type to check (e.g., "kibana")
     * @param clusterState The current cluster state
     * @return {@code false} if the type mapping needs to be added.
     */
    private boolean hasMappingType(final String type, final ClusterState clusterState) {
        final IndexMetaData dataIndex = clusterState.getMetaData().getIndices().get(MonitoringTemplateUtils.DATA_INDEX);

        // if the index does not exist, then the template will add it and the type; if the index does exist, then we need the type
        return dataIndex == null || dataIndex.getMappings().containsKey(type);
    }

    /**
     * Add the mapping {@code type} to the {@linkplain MonitoringTemplateUtils#DATA_INDEX data index}.
     *
     * @param type The data type to check (e.g., "kibana")
     * @param listener The listener to use for handling the response
     */
    private void putMappingType(final String type, final ActionListener<PutMappingResponse> listener) {
        logger.debug("adding mapping type [{}] to [{}]", type, MonitoringTemplateUtils.DATA_INDEX);

        final PutMappingRequest putMapping = new PutMappingRequest(MonitoringTemplateUtils.DATA_INDEX);

        putMapping.type(type);
        // avoid mapping at all; we use this index as a data cache rather than for search
        putMapping.source("{\"enabled\":false}", XContentType.JSON);

        client.admin().indices().putMapping(putMapping, listener);
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
    private void putIngestPipeline(ActionListener<WritePipelineResponse> listener) {
        logger.debug("installing ingest pipeline [{}]", EXPORT_PIPELINE_NAME);

        final BytesReference emptyPipeline = emptyPipeline(XContentType.JSON).bytes();
        final PutPipelineRequest request = new PutPipelineRequest(EXPORT_PIPELINE_NAME, emptyPipeline, XContentType.JSON);

        client.admin().cluster().putPipeline(request, listener);
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

    private void putTemplate(String template, String source, ActionListener<PutIndexTemplateResponse> listener) {
        logger.debug("installing template [{}]",template);

        PutIndexTemplateRequest request = new PutIndexTemplateRequest(template).source(source, XContentType.JSON);
        assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";

        // async call, so we won't block cluster event thread
        client.admin().indices().putTemplate(request, listener);
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

                MonitoringDoc monitoringDoc = new MonitoringDoc(null, null, null, null, null,
                        System.currentTimeMillis(), (MonitoringDoc.Node) null);

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

    private IndicesAliasesRequest prepareAddAliasesTo2xIndices(ClusterState clusterState) {
        IndicesAliasesRequest request = null;
        for (IndexMetaData index : clusterState.metaData()) {
            String name = index.getIndex().getName();
            if (name.startsWith(".marvel-es-1-")) {
                // we add a suffix so that it will not collide with today's monitoring index following an upgrade
                String alias = ".monitoring-es-2-" + name.substring(".marvel-es-1-".length()) + "-alias";
                if (index.getAliases().containsKey(alias)) continue;
                if (request == null) {
                    request = new IndicesAliasesRequest();
                }
                request.addAliasAction(AliasActions.add().index(name).alias(alias));
            }
        }
        return request;
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
        private final AtomicInteger countDown;

        private ResponseActionListener(String type, String name, AtomicInteger countDown) {
            this.type = Objects.requireNonNull(type);
            this.name = Objects.requireNonNull(name);
            this.countDown = Objects.requireNonNull(countDown);
        }

        @Override
        public void onResponse(Response response) {
            responseReceived();
            if (response.isAcknowledged()) {
                logger.trace("successfully set monitoring {} [{}]", type, name);
            } else {
                logger.error("failed to set monitoring index {} [{}]", type, name);
            }
        }

        @Override
        public void onFailure(Exception e) {
            responseReceived();
            logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to set monitoring index {} [{}]", type, name), e);
        }

        private void responseReceived() {
            if (countDown.decrementAndGet() <= 0) {
                logger.trace("all installation requests returned a response");
                if (installingSomething.compareAndSet(true, false) == false) {
                    throw new IllegalStateException("could not reset installing flag to false");
                }
            }
        }
    }
}
