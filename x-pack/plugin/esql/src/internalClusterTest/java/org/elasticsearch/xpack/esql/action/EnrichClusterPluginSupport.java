/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.List;

import static org.elasticsearch.test.ESTestCase.TEST_REQUEST_TIMEOUT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Helper test interface for cross-cluster IT test cases that need to exercise ENRICH. Implementers
 * are responsible for removing any other composite {@code LocalStateCompositeXPackPlugin} already
 * present in their plugin list before calling {@link #addEnrichPlugins}, since only one may be
 * registered per node.
 */
public interface EnrichClusterPluginSupport {

    /**
     * Swaps in {@link AbstractEnrichBasedCrossClusterTestCase.LocalStateEnrich} as the node's
     * composite XPack plugin and adds the plugins it needs ({@link IngestCommonPlugin},
     * {@link ReindexPlugin}), so ENRICH can be exercised.
     */
    default List<Class<? extends Plugin>> addEnrichPlugins(List<Class<? extends Plugin>> plugins) {
        plugins.add(AbstractEnrichBasedCrossClusterTestCase.LocalStateEnrich.class);
        plugins.add(IngestCommonPlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    /**
     * ENRICH policy actions (e.g. {@code PutEnrichPolicyAction}) require a security context user, which
     * is only populated when security is explicitly disabled.
     */
    default Settings enrichNodeSettings(Settings existing) {
        return Settings.builder().put(existing).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    /**
     * Creates a match enrich policy named {@code policyName} on {@code client}'s cluster, backed by an
     * index with an {@code enrich_key} (long) match field and an {@code enrich_name} (keyword) enrich
     * field. The backing index is deleted once the policy is executed, mirroring the lifecycle used by
     * {@code AbstractEnrichBasedCrossClusterTestCase#initHostsPolicy}.
     */
    default void setupEnrichPolicy(Client client, String policyName, int numDocs) {
        String sourceIndexName = policyName + "_source";
        assertAcked(
            client.admin().indices().prepareCreate(sourceIndexName).setMapping("enrich_key", "type=long", "enrich_name", "type=keyword")
        );
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(sourceIndexName).setSource("enrich_key", i, "enrich_name", "enrich_" + i).get();
        }
        client.admin().indices().prepareRefresh(sourceIndexName).get();
        EnrichPolicy policy = new EnrichPolicy("match", null, List.of(sourceIndexName), "enrich_key", List.of("enrich_name"));
        client.execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName, policy))
            .actionGet();
        client.execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName))
            .actionGet();
        assertAcked(client.admin().indices().prepareDelete(sourceIndexName));
    }

    default void deleteEnrichPolicy(Client client, String policyName) {
        try {
            client.execute(DeleteEnrichPolicyAction.INSTANCE, new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName))
                .actionGet();
        } catch (ResourceNotFoundException e) {
            // the policy was never created on this cluster - nothing to clean up
        }
    }
}
