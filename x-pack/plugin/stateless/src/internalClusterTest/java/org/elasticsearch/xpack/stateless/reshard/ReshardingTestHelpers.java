/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

public class ReshardingTestHelpers {
    public static IndexRouting postSplitRouting(ClusterState state, Index index, int targetShardCount) {
        IndexMetadata indexMetadata = indexMetadata(state, index);
        var indexMetadataPostSplit = IndexMetadata.builder(indexMetadata).reshardAddShards(targetShardCount).build();
        return IndexRouting.fromIndexMetadata(indexMetadataPostSplit);
    }

    public static String makeIdThatRoutesToShard(IndexRouting indexRouting, int shardId) {
        return makeIdThatRoutesToShard(indexRouting, shardId, "");
    }

    public static String makeIdThatRoutesToShard(IndexRouting indexRouting, int shardId, String prefix) {
        while (true) {
            String documentId = prefix + ESTestCase.randomAlphaOfLength(5);
            int routedShard = indexRouting.indexShard(new IndexRequest().id(documentId).routing(null));
            if (routedShard == shardId) {
                return documentId;
            }
        }
    }

    public static IndexMetadata indexMetadata(ClusterState state, Index index) {
        return state.metadata().indexMetadata(index);
    }
}
