/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.license.GetFeatureUsageRequest;
import org.elasticsearch.license.GetFeatureUsageResponse;
import org.elasticsearch.license.TransportGetFeatureUsageAction;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

abstract class AbstractDocumentAndFieldLevelSecurityTests extends SecurityIntegTestCase {

    private static final Set<String> DLS_FLS_FEATURE_NAMES = Set.of(
        DOCUMENT_LEVEL_SECURITY_FEATURE.getName(),
        FIELD_LEVEL_SECURITY_FEATURE.getName()
    );

    protected static void assertOnlyDlsTracked() {
        Set<String> features = fetchFeatureUsageFromAllNodes();
        assertThat(DOCUMENT_LEVEL_SECURITY_FEATURE.getName(), is(in(features)));
        assertThat(FIELD_LEVEL_SECURITY_FEATURE.getName(), not(is(in(features))));
    }

    protected static void assertOnlyFlsTracked() {
        Set<String> features = fetchFeatureUsageFromAllNodes();
        assertThat(FIELD_LEVEL_SECURITY_FEATURE.getName(), is(in(features)));
        assertThat(DOCUMENT_LEVEL_SECURITY_FEATURE.getName(), not(is(in(features))));
    }

    protected static void assertDlsFlsTracked() {
        assertThat(DLS_FLS_FEATURE_NAMES, everyItem(is(in(fetchFeatureUsageFromAllNodes()))));
    }

    protected static void assertDlsFlsNotTrackedAcrossAllNodes() {
        assertThat(fetchFeatureUsageFromAllNodes(), not(containsInAnyOrder(DLS_FLS_FEATURE_NAMES)));
    }

    protected static void assertDlsFlsNotTrackedOnCoordOnlyNode() {
        assertThat(fetchFeatureUsageFromCoordOnlyNode(), not(containsInAnyOrder(DLS_FLS_FEATURE_NAMES)));
    }

    protected static Set<String> fetchFeatureUsageFromAllNodes() {
        final Set<String> result = new HashSet<>();
        // Nodes are chosen at random when test is executed,
        // hence we have to aggregate feature usage across all nodes in the cluster.
        Set.of(internalCluster().getNodeNames()).stream().forEach(node -> { result.addAll(fetchFeatureUsageFromNode(client(node))); });
        return result;
    }

    protected static Set<String> fetchFeatureUsageFromCoordOnlyNode() {
        return fetchFeatureUsageFromNode(internalCluster().coordOnlyNodeClient());
    }

    protected static Set<String> fetchFeatureUsageFromNode(Client client) {
        final Set<String> result = new HashSet<>();
        PlainActionFuture<GetFeatureUsageResponse> listener = new PlainActionFuture<>();
        client.execute(TransportGetFeatureUsageAction.TYPE, new GetFeatureUsageRequest(), listener);
        GetFeatureUsageResponse response = listener.actionGet();
        for (var feature : response.getFeatures()) {
            result.add(feature.getName());
        }
        return result;
    }
}
