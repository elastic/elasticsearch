/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

public class TransportGetAutoFollowPatternActionTests extends ESTestCase {

    public void testGetAutoFollowPattern() {
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put(
            "name1",
            new AutoFollowPattern(
                "test_alias1",
                Collections.singletonList("index-*"),
                Collections.emptyList(),
                null,
                Settings.EMPTY,
                true,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
        patterns.put(
            "name2",
            new AutoFollowPattern(
                "test_alias1",
                Collections.singletonList("index-*"),
                Collections.emptyList(),
                null,
                Settings.EMPTY,
                true,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
        ProjectMetadata metadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap()))
            .build();

        Map<String, AutoFollowPattern> result = TransportGetAutoFollowPatternAction.getAutoFollowPattern(metadata, "name1");
        assertThat(result.size(), equalTo(1));
        assertThat(result, hasEntry("name1", patterns.get("name1")));

        result = TransportGetAutoFollowPatternAction.getAutoFollowPattern(metadata, null);
        assertThat(result.size(), equalTo(2));
        assertThat(result, hasEntry("name1", patterns.get("name1")));
        assertThat(result, hasEntry("name2", patterns.get("name2")));

        expectThrows(
            ResourceNotFoundException.class,
            () -> TransportGetAutoFollowPatternAction.getAutoFollowPattern(metadata, "another_alias")
        );
    }

    public void testGetAutoFollowPatternNoAutoFollowPatterns() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        ProjectMetadata metadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata)
            .build();
        expectThrows(ResourceNotFoundException.class, () -> TransportGetAutoFollowPatternAction.getAutoFollowPattern(metadata, "name1"));

        Map<String, AutoFollowPattern> result = TransportGetAutoFollowPatternAction.getAutoFollowPattern(metadata, null);
        assertThat(result.size(), equalTo(0));
    }

    public void testGetAutoFollowPatternNoAutoFollowMetadata() {
        ProjectMetadata metadata = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        expectThrows(ResourceNotFoundException.class, () -> TransportGetAutoFollowPatternAction.getAutoFollowPattern(metadata, "name1"));

        Map<String, AutoFollowPattern> result = TransportGetAutoFollowPatternAction.getAutoFollowPattern(metadata, null);
        assertThat(result.size(), equalTo(0));
    }

}
