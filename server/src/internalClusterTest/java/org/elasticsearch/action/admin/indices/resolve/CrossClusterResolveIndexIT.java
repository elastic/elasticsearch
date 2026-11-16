/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.AbstractMultiClustersTestCase;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

public class CrossClusterResolveIndexIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER_1 = "remote1";
    private static final String REMOTE_CLUSTER_2 = "remote2";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    public void testSameIndexNameInDifferentModesOnTwoRemotes() {

        String indexName = "shared";
        String remote1Index = createIndex(REMOTE_CLUSTER_1, indexName, IndexMode.STANDARD);
        String remote2Index = createIndex(REMOTE_CLUSTER_2, indexName, IndexMode.LOOKUP);

        assertThat(resolveIndices("*:shared", EnumSet.noneOf(IndexMode.class)), equalTo(Set.of(remote1Index, remote2Index)));
        assertThat(resolveIndices("*:shared", EnumSet.of(IndexMode.STANDARD)), equalTo(Set.of(remote1Index)));
        assertThat(resolveIndices("*:shared", EnumSet.of(IndexMode.LOOKUP)), equalTo(Set.of(remote2Index)));
        assertThat(
            resolveIndices("*:shared", EnumSet.of(IndexMode.STANDARD, IndexMode.LOOKUP)),
            equalTo(Set.of(remote1Index, remote2Index))
        );
        assertThat(resolveIndices("*:shared", EnumSet.of(IndexMode.TIME_SERIES)), emptyIterable());
    }

    public void testSameAliasNameInDifferentModesOnTwoRemotes() {
        String aliasName = "shared-alias";
        String remote1Index = createIndex(REMOTE_CLUSTER_1, "standard-index", IndexMode.STANDARD);
        String remote2Index = createIndex(REMOTE_CLUSTER_2, "lookup-index", IndexMode.LOOKUP);
        String remote1Alias = createAlias(REMOTE_CLUSTER_1, "standard-index", aliasName);
        String remote2Alias = createAlias(REMOTE_CLUSTER_2, "lookup-index", aliasName);

        assertThat(
            resolveIndices("*:*", EnumSet.noneOf(IndexMode.class)),
            equalTo(Set.of(remote1Alias, remote1Index, remote2Alias, remote2Index))
        );
        assertThat(resolveIndices("*:*", EnumSet.of(IndexMode.STANDARD)), equalTo(Set.of(remote1Alias, remote1Index)));
        assertThat(resolveIndices("*:*", EnumSet.of(IndexMode.LOOKUP)), equalTo(Set.of(remote2Alias, remote2Index)));
        assertThat(
            resolveIndices("*:*", EnumSet.of(IndexMode.STANDARD, IndexMode.LOOKUP)),
            equalTo(Set.of(remote1Alias, remote1Index, remote2Alias, remote2Index))
        );
        assertThat(resolveIndices("*:*", EnumSet.of(IndexMode.TIME_SERIES)), emptyIterable());
    }

    private Set<String> resolveIndices(String pattern, EnumSet<IndexMode> indexModes) {
        var request = new ResolveIndexAction.Request(
            new String[] { pattern },
            ResolveIndexAction.Request.DEFAULT_INDICES_OPTIONS,
            indexModes
        );
        var response = client(LOCAL_CLUSTER).admin().indices().resolveIndex(request).actionGet();
        return Stream.concat(response.getIndices().stream(), response.getAliases().stream())
            .map(ResolveIndexAction.ResolvedIndexAbstraction::getName)
            .collect(toSet());
    }

    private String createIndex(String clusterAlias, String indexName, IndexMode mode) {
        var settings = Settings.builder().put("index.number_of_shards", 1).put(IndexSettings.MODE.getKey(), mode.getName());
        assertAcked(client(clusterAlias).admin().indices().prepareCreate(indexName).setSettings(settings));
        return clusterAlias + ":" + indexName;
    }

    private String createAlias(String clusterAlias, String indexName, String aliasName) {
        assertAcked(
            client(clusterAlias).admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias(indexName, aliasName)
        );
        return clusterAlias + ":" + aliasName;
    }
}
