/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

import static org.elasticsearch.test.LambdaMatchers.falseWith;
import static org.elasticsearch.test.LambdaMatchers.trueWith;
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
        assertThat(matcher, trueWith("index1"));
        assertThat(matcher, trueWith("ind*x1"));
        assertThat(matcher, falseWith("index2"));

        assertThat(matcher, trueWith("alias"));
        assertThat(matcher, trueWith("*lias"));

        assertThat(matcher, falseWith("cluster:index1"));
    }

    public void testRemoteIndex() {
        assertThat(remoteMatcher, trueWith("cluster:index1"));
        assertThat(remoteMatcher, trueWith("cluster:ind*x1"));
        assertThat(remoteMatcher, trueWith("*luster:ind*x1"));
        assertThat(remoteMatcher, falseWith("cluster:index2"));

        assertThat(remoteMatcher, trueWith("cluster:alias"));
        assertThat(remoteMatcher, trueWith("cluster:*lias"));

        assertThat(remoteMatcher, falseWith("index1"));
        assertThat(remoteMatcher, falseWith("alias"));

        assertThat(remoteMatcher, falseWith("*index1"));
        assertThat(remoteMatcher, falseWith("*alias"));
        assertThat(remoteMatcher, falseWith("cluster*"));
        assertThat(remoteMatcher, falseWith("cluster*index1"));
    }
}
