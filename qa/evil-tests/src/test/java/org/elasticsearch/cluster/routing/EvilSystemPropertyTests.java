/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class EvilSystemPropertyTests extends ESTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testDisableSearchAllocationAwareness() {
        Settings indexSettings = Settings.builder().put("cluster.routing.allocation.awareness.attributes", "test").build();
        OperationRouting routing = new OperationRouting(
            indexSettings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertWarnings(OperationRouting.IGNORE_AWARENESS_ATTRIBUTES_DEPRECATION_MESSAGE);
        assertThat(routing.getAwarenessAttributes().size(), equalTo(1));
        assertThat(routing.getAwarenessAttributes().get(0), equalTo("test"));
        System.setProperty("es.search.ignore_awareness_attributes", "true");
        try {
            routing = new OperationRouting(indexSettings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            assertTrue(routing.getAwarenessAttributes().isEmpty());
        } finally {
            System.clearProperty("es.search.ignore_awareness_attributes");
        }

    }
}
