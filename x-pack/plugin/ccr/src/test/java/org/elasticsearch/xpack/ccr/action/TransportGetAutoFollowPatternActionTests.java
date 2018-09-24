/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.MetaData;
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
        patterns.put("test_alias1",
            new AutoFollowPattern(Collections.singletonList("index-*"), null, null, null, null, null, null, null, null));
        patterns.put("test_alias2",
            new AutoFollowPattern(Collections.singletonList("index-*"), null, null, null, null, null, null, null, null));
        MetaData metaData = MetaData.builder()
            .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap()))
            .build();

        Map<String, AutoFollowPattern> result = TransportGetAutoFollowPatternAction.getAutoFollowPattern(metaData, "test_alias1");
        assertThat(result.size(), equalTo(1));
        assertThat(result, hasEntry("test_alias1", patterns.get("test_alias1")));

        result = TransportGetAutoFollowPatternAction.getAutoFollowPattern(metaData, null);
        assertThat(result.size(), equalTo(2));
        assertThat(result, hasEntry("test_alias1", patterns.get("test_alias1")));
        assertThat(result, hasEntry("test_alias2", patterns.get("test_alias2")));

        expectThrows(ResourceNotFoundException.class,
            () -> TransportGetAutoFollowPatternAction.getAutoFollowPattern(metaData, "another_alias"));
    }

    public void testGetAutoFollowPatternNoAutoFollowPatterns() {
        AutoFollowMetadata autoFollowMetadata =
            new AutoFollowMetadata(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        MetaData metaData = MetaData.builder()
            .putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata)
            .build();
        expectThrows(ResourceNotFoundException.class,
            () -> TransportGetAutoFollowPatternAction.getAutoFollowPattern(metaData, "test_alias"));
    }

    public void testGetAutoFollowPatternNoAutoFollowMetadata() {
        MetaData metaData = MetaData.builder().build();
        expectThrows(ResourceNotFoundException.class,
            () -> TransportGetAutoFollowPatternAction.getAutoFollowPattern(metaData, "test_alias"));
    }

}
