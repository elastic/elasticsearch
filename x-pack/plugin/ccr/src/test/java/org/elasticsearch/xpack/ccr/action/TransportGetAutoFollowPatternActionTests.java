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
import java.util.Map;

import static org.hamcrest.Matchers.sameInstance;

public class TransportGetAutoFollowPatternActionTests extends ESTestCase {

    public void testGetAutoFollowPattern() {
        Map<String, AutoFollowPattern> patterns = Collections.singletonMap("test_alias",
            new AutoFollowPattern(Collections.singletonList("index-*"), null, null, null, null, null, null, null, null, null));
        MetaData metaData = MetaData.builder()
            .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, Collections.emptyMap()))
            .build();

        AutoFollowPattern result = TransportGetAutoFollowPatternAction.getAutoFollowPattern(metaData, "test_alias");
        assertThat(result, sameInstance(patterns.get("test_alias")));

        expectThrows(ResourceNotFoundException.class,
            () -> TransportGetAutoFollowPatternAction.getAutoFollowPattern(metaData, "another_alias"));
    }

    public void testGetAutoFollowPatternNoAutoFollowPatterns() {
        MetaData metaData = MetaData.builder()
            .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(Collections.emptyMap(), Collections.emptyMap()))
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
