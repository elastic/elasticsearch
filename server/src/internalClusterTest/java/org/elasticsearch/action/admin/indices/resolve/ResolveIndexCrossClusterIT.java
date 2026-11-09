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
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

public class ResolveIndexCrossClusterIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER_1 = "remote1";
    private static final String REMOTE_CLUSTER_2 = "remote2";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    /**
     * Two remotes hold an index with the same name but a different mode. The mode filter must apply to each remote
     * independently, so the name alone must never decide whether an index is kept.
     */
    public void testSameIndexNameInDifferentModesOnTwoRemotes() {

        String indexName = "shared";
        String remote1Index = createIndex(REMOTE_CLUSTER_1, indexName, IndexMode.STANDARD);
        String remote2Index = createIndex(REMOTE_CLUSTER_2, indexName, IndexMode.LOOKUP);

        assertThat(
            resolveModes("*:shared", EnumSet.noneOf(IndexMode.class)),
            equalTo(Map.of(remote1Index, IndexMode.STANDARD, remote2Index, IndexMode.LOOKUP))
        );
        assertThat(resolveModes("*:shared", EnumSet.of(IndexMode.STANDARD)), equalTo(Map.of(remote1Index, IndexMode.STANDARD)));
        assertThat(resolveModes("*:shared", EnumSet.of(IndexMode.LOOKUP)), equalTo(Map.of(remote2Index, IndexMode.LOOKUP)));
        assertThat(
            resolveModes("*:shared", EnumSet.of(IndexMode.STANDARD, IndexMode.LOOKUP)),
            equalTo(Map.of(remote1Index, IndexMode.STANDARD, remote2Index, IndexMode.LOOKUP))
        );
        assertThat(resolveModes("*:shared", EnumSet.of(IndexMode.TIME_SERIES)), anEmptyMap());
    }

    /**
     * Resolves {@code *:shared} from the local cluster and returns the mode of every resolved index by its name.
     */
    private Map<String, IndexMode> resolveModes(String pattern, EnumSet<IndexMode> indexModes) {
        var request = new ResolveIndexAction.Request(
            new String[] { pattern },
            ResolveIndexAction.Request.DEFAULT_INDICES_OPTIONS,
            indexModes
        );
        var response = client(LOCAL_CLUSTER).admin().indices().resolveIndex(request).actionGet();
        assertThat(response.getAliases(), equalTo(List.of()));
        assertThat(response.getDataStreams(), equalTo(List.of()));
        return response.getIndices()
            .stream()
            .collect(Collectors.toMap(ResolveIndexAction.ResolvedIndex::getName, ResolveIndexAction.ResolvedIndex::getMode));
    }

    private String createIndex(String clusterAlias, String indexName, IndexMode mode) {
        // lookup indices must have a single shard
        var settings = Settings.builder().put("index.number_of_shards", 1).put(IndexSettings.MODE.getKey(), mode.getName());
        assertAcked(client(clusterAlias).admin().indices().prepareCreate(indexName).setSettings(settings));
        return clusterAlias + ":" + indexName;
    }
}
