/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static java.util.Collections.singletonMap;

/**
 * Upgrades Templates on behalf of installed {@link Plugin}s when a node joins the cluster
 */
public class TemplateUpgradeService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TemplateUpgradeService.class);

    private final UnaryOperator<Map<String, IndexTemplateMetadata>> indexTemplateMetadataUpgraders;

    public final ClusterService clusterService;

    public final ThreadPool threadPool;

    public final Client client;

    final AtomicInteger upgradesInProgress = new AtomicInteger();

    private Map<String, IndexTemplateMetadata> lastTemplateMetadata;

    public TemplateUpgradeService(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexTemplateMetadataUpgraders = templates -> {
            Map<String, IndexTemplateMetadata> upgradedTemplates = new HashMap<>(templates);
            for (UnaryOperator<Map<String, IndexTemplateMetadata>> upgrader : indexTemplateMetadataUpgraders) {
                upgradedTemplates = upgrader.apply(upgradedTemplates);
            }
            return upgradedTemplates;
        };
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have the index templates,
            // while they actually do exist
            return;
        }

        if (upgradesInProgress.get() > 0) {
            // we are already running some upgrades - skip this cluster state update
            return;
        }

        Map<String, IndexTemplateMetadata> templates = state.getMetadata().getTemplates();

        if (templates == lastTemplateMetadata) {
            // we already checked these sets of templates - no reason to check it again
            // we can do identity check here because due to cluster state diffs the actual map will not change
            // if there were no changes
            return;
        }

        lastTemplateMetadata = templates;
        Optional<Tuple<Map<String, BytesReference>, Set<String>>> changes = calculateTemplateChanges(templates);
        if (changes.isPresent()) {
            if (upgradesInProgress.compareAndSet(0, changes.get().v1().size() + changes.get().v2().size() + 1)) {
                logger.info(
                    "Starting template upgrade to version {}, {} templates will be updated and {} will be removed",
                    Build.current().version(),
                    changes.get().v1().size(),
                    changes.get().v2().size()
                );

                assert threadPool.getThreadContext().isSystemContext();
                threadPool.generic().execute(() -> upgradeTemplates(changes.get().v1(), changes.get().v2()));
            }
        }
    }

    void upgradeTemplates(Map<String, BytesReference> changes, Set<String> deletions) {
        final AtomicBoolean anyUpgradeFailed = new AtomicBoolean(false);
        if (threadPool.getThreadContext().isSystemContext() == false) {
            throw new IllegalStateException("template updates from the template upgrade service should always happen in a system context");
        }

        for (Map.Entry<String, BytesReference> change : changes.entrySet()) {
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(change.getKey()).source(change.getValue(), XContentType.JSON);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            client.admin().indices().putTemplate(request, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    if (response.isAcknowledged() == false) {
                        anyUpgradeFailed.set(true);
                        logger.warn("Error updating template [{}], request was not acknowledged", change.getKey());
                    }
                    tryFinishUpgrade(anyUpgradeFailed);
                }

                @Override
                public void onFailure(Exception e) {
                    anyUpgradeFailed.set(true);
                    logger.warn(() -> "Error updating template [" + change.getKey() + "]", e);
                    tryFinishUpgrade(anyUpgradeFailed);
                }
            });
        }

        for (String template : deletions) {
            DeleteIndexTemplateRequest request = new DeleteIndexTemplateRequest(template);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            client.admin().indices().deleteTemplate(request, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    if (response.isAcknowledged() == false) {
                        anyUpgradeFailed.set(true);
                        logger.warn("Error deleting template [{}], request was not acknowledged", template);
                    }
                    tryFinishUpgrade(anyUpgradeFailed);
                }

                @Override
                public void onFailure(Exception e) {
                    anyUpgradeFailed.set(true);
                    if (e instanceof IndexTemplateMissingException == false) {
                        // we might attempt to delete the same template from different nodes - so that's ok if template doesn't exist
                        // otherwise we need to warn
                        logger.warn(() -> "Error deleting template [" + template + "]", e);
                    }
                    tryFinishUpgrade(anyUpgradeFailed);
                }
            });
        }
    }

    void tryFinishUpgrade(AtomicBoolean anyUpgradeFailed) {
        assert upgradesInProgress.get() > 0;
        if (upgradesInProgress.decrementAndGet() == 1) {
            try {
                // this is the last upgrade, the templates should now be in the desired state
                if (anyUpgradeFailed.get()) {
                    logger.info("Templates were partially upgraded to version {}", Build.current().version());
                } else {
                    logger.info("Templates were upgraded successfully to version {}", Build.current().version());
                }
                // Check upgraders are satisfied after the update completed. If they still
                // report that changes are required, this might indicate a bug or that something
                // else tinkering with the templates during the upgrade.
                final Map<String, IndexTemplateMetadata> upgradedTemplates = clusterService.state().getMetadata().getTemplates();
                final boolean changesRequired = calculateTemplateChanges(upgradedTemplates).isPresent();
                if (changesRequired) {
                    logger.warn("Templates are still reported as out of date after the upgrade. The template upgrade will be retried.");
                }
            } finally {
                final int noMoreUpgrades = upgradesInProgress.decrementAndGet();
                assert noMoreUpgrades == 0;
            }
        }
    }

    Optional<Tuple<Map<String, BytesReference>, Set<String>>> calculateTemplateChanges(Map<String, IndexTemplateMetadata> templates) {
        // collect current templates
        Map<String, IndexTemplateMetadata> existingMap = new HashMap<>();
        for (Map.Entry<String, IndexTemplateMetadata> customCursor : templates.entrySet()) {
            existingMap.put(customCursor.getKey(), customCursor.getValue());
        }
        // upgrade global custom meta data
        Map<String, IndexTemplateMetadata> upgradedMap = indexTemplateMetadataUpgraders.apply(existingMap);
        if (upgradedMap.equals(existingMap) == false) {
            Set<String> deletes = new HashSet<>();
            Map<String, BytesReference> changes = new HashMap<>();
            // remove templates if needed
            existingMap.keySet().forEach(s -> {
                if (upgradedMap.containsKey(s) == false) {
                    deletes.add(s);
                }
            });
            upgradedMap.forEach((key, value) -> {
                if (value.equals(existingMap.get(key)) == false) {
                    changes.put(key, toBytesReference(value));
                }
            });
            return Optional.of(new Tuple<>(changes, deletes));
        }
        return Optional.empty();
    }

    private static final ToXContent.Params PARAMS = new ToXContent.MapParams(singletonMap("reduce_mappings", "true"));

    private static BytesReference toBytesReference(IndexTemplateMetadata templateMetadata) {
        try {
            return XContentHelper.toXContent((builder, params) -> {
                IndexTemplateMetadata.Builder.toInnerXContentWithTypes(templateMetadata, builder, params);
                return builder;
            }, XContentType.JSON, PARAMS, false);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot serialize template [" + templateMetadata.getName() + "]", ex);
        }
    }
}
