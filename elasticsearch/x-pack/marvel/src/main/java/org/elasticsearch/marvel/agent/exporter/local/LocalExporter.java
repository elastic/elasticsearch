/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.marvel.agent.resolver.ResolversRegistry;
import org.elasticsearch.marvel.cleaner.CleanerService;
import org.elasticsearch.marvel.support.init.proxy.MonitoringClientProxy;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;

/**
 *
 */
public class LocalExporter extends Exporter implements ClusterStateListener, CleanerService.Listener {

    public static final String TYPE = "local";

    private final MonitoringClientProxy client;
    private final ClusterService clusterService;
    private final ResolversRegistry resolvers;
    private final CleanerService cleanerService;

    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);

    public LocalExporter(Exporter.Config config, MonitoringClientProxy client,
                         ClusterService clusterService, CleanerService cleanerService) {
        super(TYPE, config);
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
            logger.debug("stopped");
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

            logger.debug("monitoring index templates are installed, service can start");

        } else {

            // we are on master
            //
            // Check that there is nothing that could block metadata updates
            if (clusterState.blocks().hasGlobalBlock(ClusterBlockLevel.METADATA_WRITE)) {
                logger.debug("waiting until metadata writes are unblocked");
                return null;
            }

            // Check that each required template exist, installing it if needed
            for (Map.Entry<String, String> template : templates.entrySet()) {
                if (hasTemplate(template.getKey(), clusterState) == false) {
                    logger.debug("template [{}] not found", template.getKey());
                    putTemplate(template.getKey(), template.getValue());
                    return null;
                } else {
                    logger.debug("template [{}] found", template.getKey());
                }
            }

            logger.debug("monitoring index templates are installed on master node, service can start");
        }

        if (state.compareAndSet(State.INITIALIZED, State.RUNNING)) {
            logger.debug("started");
        }
        return new LocalBulk(name(), logger, client, resolvers);
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

    void putTemplate(String template, String source) {
        logger.debug("installing template [{}]",template);

        PutIndexTemplateRequest request = new PutIndexTemplateRequest(template).source(source);
        assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";

        // async call, so we won't block cluster event thread
        client.admin().indices().putTemplate(request, new ActionListener<PutIndexTemplateResponse>() {
            @Override
            public void onResponse(PutIndexTemplateResponse response) {
                if (response.isAcknowledged()) {
                    logger.trace("successfully installed monitoring template [{}]", template);
                } else {
                    logger.error("failed to update monitoring index template [{}]", template);
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                logger.error("failed to update monitoring index template [{}]", throwable, template);
            }
        });
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
            public void onFailure(Throwable e) {
                logger.error("failed to delete indices", e);
            }
        });
    }

    public static class Factory extends Exporter.Factory<LocalExporter> {

        private final MonitoringClientProxy client;
        private final ClusterService clusterService;
        private final CleanerService cleanerService;

        @Inject
        public Factory(MonitoringClientProxy client, ClusterService clusterService, CleanerService cleanerService) {
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

    enum State {
        INITIALIZED,
        RUNNING,
        TERMINATED
    }
}
