/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import static org.hamcrest.Matchers.equalTo;

public class ClusterDeprecationChecksTests extends ESTestCase {

    public void testCheckTransientSettingsExistence() {
        Settings persistentSettings = Settings.builder().put("xpack.monitoring.collection.enabled", true).build();

        Settings transientSettings = Settings.builder()
            .put("indices.recovery.max_bytes_per_sec", "20mb")
            .put("action.auto_create_index", true)
            .put("cluster.routing.allocation.enable", "primaries")
            .build();
        Metadata metadataWithTransientSettings = Metadata.builder()
            .persistentSettings(persistentSettings)
            .transientSettings(transientSettings)
            .build();

        ClusterState badState = ClusterState.builder(new ClusterName("test")).metadata(metadataWithTransientSettings).build();
        DeprecationIssue issue = ClusterDeprecationChecks.checkTransientSettingsExistence(badState);
        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Transient cluster settings are deprecated",
                    "https://ela.st/es-deprecation-7-transient-cluster-settings",
                    "Use of transient settings is deprecated. Some Elastic products "
                        + "may make use of transient settings and those should not be changed. "
                        + "Any custom use of transient settings should be replaced by persistent settings.",
                    false,
                    null
                )
            )
        );

        persistentSettings = Settings.builder().put("indices.recovery.max_bytes_per_sec", "20mb").build();
        Metadata metadataWithoutTransientSettings = Metadata.builder().persistentSettings(persistentSettings).build();

        ClusterState okState = ClusterState.builder(new ClusterName("test")).metadata(metadataWithoutTransientSettings).build();
        issue = ClusterDeprecationChecks.checkTransientSettingsExistence(okState);
        assertNull(issue);
    }
}
