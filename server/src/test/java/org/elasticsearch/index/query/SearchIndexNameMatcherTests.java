/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchIndexNameMatcherTests extends ESTestCase {
    private SearchIndexNameMatcher matcher;
    private SearchIndexNameMatcher remoteMatcher;

    @Before
    public void setUpMatchers() {
        Metadata.Builder metadataBuilder = Metadata.builder()
            .put(indexBuilder("index1").putAlias(AliasMetadata.builder("alias")))
            .put(indexBuilder("index2").putAlias(AliasMetadata.builder("alias")))
            .put(indexBuilder("index3"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadataBuilder).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        matcher = new SearchIndexNameMatcher("index1", "", clusterService, TestIndexNameExpressionResolver.newInstance());
        remoteMatcher = new SearchIndexNameMatcher("index1", "cluster", clusterService, TestIndexNameExpressionResolver.newInstance());
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return IndexMetadata.builder(index).settings(indexSettings(IndexVersion.current(), 1, 0));
    }

    public void testLocalIndex() {
        assertTrue(matcher.test("index1"));
        assertTrue(matcher.test("ind*x1"));
        assertFalse(matcher.test("index2"));

        assertTrue(matcher.test("alias"));
        assertTrue(matcher.test("*lias"));

        assertFalse(matcher.test("cluster:index1"));
    }

    public void testRemoteIndex() {
        assertTrue(remoteMatcher.test("cluster:index1"));
        assertTrue(remoteMatcher.test("cluster:ind*x1"));
        assertTrue(remoteMatcher.test("*luster:ind*x1"));
        assertFalse(remoteMatcher.test("cluster:index2"));

        assertTrue(remoteMatcher.test("cluster:alias"));
        assertTrue(remoteMatcher.test("cluster:*lias"));

        assertFalse(remoteMatcher.test("index1"));
        assertFalse(remoteMatcher.test("alias"));

        assertFalse(remoteMatcher.test("*index1"));
        assertFalse(remoteMatcher.test("*alias"));
        assertFalse(remoteMatcher.test("cluster*"));
        assertFalse(remoteMatcher.test("cluster*index1"));
    }
}
