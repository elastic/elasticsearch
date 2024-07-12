/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.cluster.routing.remote;


import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.gateway.remote.ClusterMetadataManifest;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @opensearch.internal
 */
public interface RemoteRoutingTableService extends LifecycleComponent {
    @SuppressWarnings("checkstyle:RedundantModifier")
    public static final DiffableUtils.NonDiffableValueSerializer<String, IndexRoutingTable> CUSTOM_ROUTING_TABLE_VALUE_SERIALIZER =
        new DiffableUtils.NonDiffableValueSerializer<String, IndexRoutingTable>() {
            @Override
            public void write(IndexRoutingTable value, StreamOutput out) throws IOException {
                value.writeTo(out);
            }

            @Override
            public IndexRoutingTable read(StreamInput in, String key) throws IOException {
                return IndexRoutingTable.readFrom(in);
            }
        };

    List<IndexRoutingTable> getIndicesRouting(RoutingTable routingTable);

    CheckedRunnable<IOException> getAsyncIndexRoutingReadAction(
        String uploadedFilename,
        Index index,
        LatchedActionListener<IndexRoutingTable> latchedActionListener
    );

    List<ClusterMetadataManifest.UploadedIndexMetadata> getUpdatedIndexRoutingTableMetadata(
        List<String> updatedIndicesRouting,
        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndicesRouting
    );

    DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> getIndicesRoutingMapDiff(
        RoutingTable before,
        RoutingTable after
    );

    CheckedRunnable<IOException> getIndexRoutingAsyncAction(
        ClusterState clusterState,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener,
        BlobPath clusterBasePath
    );

    List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(
        ClusterMetadataManifest previousManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUploaded,
        List<String> indicesRoutingToDelete
    );

    @SuppressWarnings("checkstyle:RedundantModifier")
    public void deleteStaleIndexRoutingPaths(List<String> stalePaths) throws IOException;
}
