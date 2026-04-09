/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
