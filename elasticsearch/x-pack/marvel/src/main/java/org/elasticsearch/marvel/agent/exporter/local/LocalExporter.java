/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.marvel.MarvelSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.marvel.agent.resolver.ResolversRegistry;
import org.elasticsearch.marvel.cleaner.CleanerService;
import org.elasticsearch.shield.InternalClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;

/**
 *
 */
public class LocalExporter extends Exporter implements ClusterStateListener, CleanerService.Listener {

    public static final String TYPE = "local";

    private final Client client;
    private final ClusterService clusterService;
    private final ResolversRegistry resolvers;
    private final CleanerService cleanerService;

    private volatile LocalBulk bulk;
    private volatile boolean active = true;

    /** Version number of built-in templates **/
    private final Integer templateVersion;

    public LocalExporter(Exporter.Config config, Client client, ClusterService clusterService, CleanerService cleanerService) {
        super(TYPE, config);
        this.client = client;
        this.clusterService = clusterService;
        this.cleanerService = cleanerService;

        // Loads the current version number of built-in templates
        templateVersion = MarvelTemplateUtils.TEMPLATE_VERSION;
        if (templateVersion == null) {
            throw new IllegalStateException("unable to find built-in template version");
        }

        resolvers = new ResolversRegistry(config.settings());
        bulk = resolveBulk(clusterService.state(), bulk);
        clusterService.add(this);
        cleanerService.add(this);
    }

    LocalBulk getBulk() {
        return bulk;
    }

    ResolversRegistry getResolvers() {
        return resolvers;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        LocalBulk currentBulk = bulk;
        LocalBulk newBulk = resolveBulk(event.state(), currentBulk);

        // yes, this method will always be called by the cluster event loop thread
        // but we need to sync with the {@code #close()} mechanism
        synchronized (this) {
            if (active) {
                bulk = newBulk;
            } else if (newBulk != null) {
                newBulk.terminate();
            }
            if (currentBulk == null && bulk != null) {
                logger.debug("local exporter [{}] - started!", name());
            }
            if (bulk != currentBulk && currentBulk != null) {
                logger.debug("local exporter [{}] - stopped!", name());
                currentBulk.terminate();
            }
        }
    }

    @Override
    public ExportBulk openBulk() {
        return bulk;
    }

    // requires synchronization due to cluster state update events (see above)
    @Override
    public synchronized void close() {
        active = false;
        clusterService.remove(this);
        cleanerService.remove(this);
        if (bulk != null) {
            try {
                bulk.terminate();
                bulk = null;
            } catch (Exception e) {
                logger.error("local exporter [{}] - failed to cleanly close bulk", e, name());
            }
        }
    }

    LocalBulk resolveBulk(ClusterState clusterState, LocalBulk currentBulk) {
        if (clusterService.localNode() == null || clusterState == null) {
            return currentBulk;
        }

        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .monitoring-es-
            // indices but they may not have been restored from the cluster state on disk
            logger.debug("local exporter [{}] - waiting until gateway has recovered from disk", name());
            return null;
        }

        String templateName = MarvelTemplateUtils.indexTemplateName(templateVersion);
        boolean templateInstalled = hasTemplate(templateName, clusterState);

        // if this is not the master, we'll just look to see if the monitoring timestamped template is already
        // installed and if so, if it has a compatible version. If it is (installed and compatible)
        // we'll be able to start this exporter. Otherwise, we'll just wait for a new cluster state.
        if (!clusterService.localNode().masterNode()) {
            // We only need to check the index template for timestamped indices
            if (!templateInstalled) {
                // the template for timestamped indices is not yet installed in the given cluster state, we'll wait.
                logger.debug("local exporter [{}] - monitoring index template does not exist, so service cannot start", name());
                return null;
            }

            // ok.. we have a compatible template... we can start
            logger.debug("local exporter [{}] - started!", name());
            return currentBulk != null ? currentBulk : new LocalBulk(name(), logger, client, resolvers);
        }

        // we are on master
        //
        // Check that there is nothing that could block metadata updates
        if (clusterState.blocks().hasGlobalBlock(ClusterBlockLevel.METADATA_WRITE)) {
            logger.debug("local exporter [{}] - waiting until metadata writes are unblocked", name());
            return null;
        }

        // Install the index template for timestamped indices first, so that other nodes can ship data
        if (!templateInstalled) {
            logger.debug("local exporter [{}] - could not find existing monitoring template for timestamped indices, installing a new one",
                    name());
            putTemplate(templateName, MarvelTemplateUtils.loadTimestampedIndexTemplate());
            // we'll get that template on the next cluster state update
            return null;
        }

        // Install the index template for data index
        templateName = MarvelTemplateUtils.dataTemplateName(templateVersion);
        if (!hasTemplate(templateName, clusterState)) {
            logger.debug("local exporter [{}] - could not find existing monitoring template for data index, installing a new one", name());
            putTemplate(templateName, MarvelTemplateUtils.loadDataIndexTemplate());
            // we'll get that template on the next cluster state update
            return null;
        }

        // ok.. we have a compatible templates... we can start
        return currentBulk != null ? currentBulk : new LocalBulk(name(), logger, client, resolvers);
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
                    logger.debug("found index template [{}] in version [{}]", template, version);
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

    void putTemplate(String template, byte[] source) {
        logger.debug("local exporter [{}] - installing template [{}]", name(), template);

        PutIndexTemplateRequest request = new PutIndexTemplateRequest(template).source(source);
        assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";

        // async call, so we won't block cluster event thread
        client.admin().indices().putTemplate(request, new ActionListener<PutIndexTemplateResponse>() {
            @Override
            public void onResponse(PutIndexTemplateResponse response) {
                if (response.isAcknowledged()) {
                    logger.trace("local exporter [{}] - successfully installed monitoring template [{}]", name(), template);
                } else {
                    logger.error("local exporter [{}] - failed to update monitoring index template [{}]", name(), template);
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                logger.error("local exporter [{}] - failed to update monitoring index template [{}]", throwable, name(), template);
            }
        });
    }

    @Override
    public void onCleanUpIndices(TimeValue retention) {
        if (bulk == null) {
            logger.debug("local exporter [{}] - not ready yet", name());
            return;
        }

        if (clusterService.localNode().masterNode()) {

            // Retention duration can be overridden at exporter level
            TimeValue exporterRetention = config.settings().getAsTime(MarvelSettings.HISTORY_DURATION_SETTING_NAME, null);
            if (exporterRetention != null) {
                try {
                    cleanerService.validateRetention(exporterRetention);
                    retention = exporterRetention;
                } catch (IllegalArgumentException e) {
                    logger.warn("local exporter [{}] - unable to use custom history duration [{}]: {}", name(), exporterRetention,
                            e.getMessage());
                }
            }

            // Reference date time will be compared to index.creation_date settings,
            // that's why it must be in UTC
            DateTime expiration = new DateTime(DateTimeZone.UTC).minus(retention.millis());
            logger.debug("local exporter [{}] - cleaning indices [expiration={}, retention={}]", name(), expiration, retention);

            ClusterState clusterState = clusterService.state();
            if (clusterState != null) {
                long expirationTime = expiration.getMillis();

                // Get the list of monitoring index patterns
                String[] patterns = StreamSupport.stream(getResolvers().spliterator(), false)
                                                .map(MonitoringIndexNameResolver::indexPattern)
                                                .distinct()
                                                .toArray(String[]::new);

                MonitoringDoc monitoringDoc = new MonitoringDoc(MonitoredSystem.ES.getSystem(), Version.CURRENT.toString());
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
                                logger.debug("local exporter [{}] - detected expired index [name={}, created={}, expired={}]", name(),
                                        indexName, new DateTime(creationDate, DateTimeZone.UTC), expiration);
                            }
                            indices.add(indexName);
                        }
                    }
                }

                if (!indices.isEmpty()) {
                    logger.info("local exporter [{}] - cleaning up [{}] old indices", name(), indices.size());
                    deleteIndices(indices);
                } else {
                    logger.debug("local exporter [{}] - no old indices found for clean up", name());
                }
            }
        }
    }

    private void deleteIndices(Set<String> indices) {
        logger.trace("local exporter [{}] - deleting {} indices: {}", name(), indices.size(), collectionToCommaDelimitedString(indices));
        client.admin().indices().delete(new DeleteIndexRequest(indices.toArray(new String[indices.size()])),
                new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse response) {
                if (response.isAcknowledged()) {
                    logger.debug("local exporter [{}] - indices deleted", name());
                } else {
                    logger.warn("local exporter [{}] - unable to delete {} indices", name(), indices.size());
                }
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("local exporter [{}] - failed to delete indices", e, name());
            }
        });
    }

    public static class Factory extends Exporter.Factory<LocalExporter> {

        private final InternalClient client;
        private final ClusterService clusterService;
        private final CleanerService cleanerService;

        @Inject
        public Factory(InternalClient client, ClusterService clusterService, CleanerService cleanerService) {
            super(TYPE, true);
            this.client = client;
            this.clusterService = clusterService;
            this.cleanerService = cleanerService;
        }

        @Override
        public LocalExporter create(Config config) {
            return new LocalExporter(config, client, clusterService, cleanerService);
        }
    }
}
