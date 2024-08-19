/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * <p>This package exposes the blobstore repository used by Elasticsearch Snapshots.</p>
 *
 * <h2>Preliminaries</h2>
 *
 * <p>The {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} forms the basis of implementations of
 * {@link org.elasticsearch.repositories.Repository} on top of a blob store. A blobstore can be used as the basis for an implementation
 * as long as it provides for GET, PUT, DELETE, and LIST operations. For a read-only repository, it suffices if the blobstore provides only
 * GET operations.
 * These operations are formally defined as specified by the {@link org.elasticsearch.common.blobstore.BlobContainer} interface that
 * any {@code BlobStoreRepository} implementation must provide via its implementation of
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository#getBlobContainer()}.</p>
 *
 * <p>The blob store is written to and read from by master-eligible nodes and data nodes. All metadata related to a snapshot's
 * scope and health is written by the master node.</p>
 * <p>The data-nodes on the other hand, write the data for each individual shard but do not write any blobs outside of shard directories for
 * shards that they hold the primary of. For each shard, the data-node holding the shard's primary writes the actual data in form of
 * the shard's segment files to the repository as well as metadata about all the segment files that the repository stores for the shard.</p>
 *
 * <p>For the specifics on how the operations on the repository documented below are invoked during the snapshot process please refer to
 * the documentation of the {@link org.elasticsearch.snapshots} package.</p>
 *
 * <p>{@code BlobStoreRepository} maintains the following structure of blobs containing data and metadata in the blob store. The exact
 * operations executed on these blobs are explained below.</p>
 * <pre>
 * {@code
 *   STORE_ROOT
 *   |- index-N           - JSON serialized {@link org.elasticsearch.repositories.RepositoryData} containing a list of all snapshot ids
 *   |                      and the indices belonging to each snapshot, N is the generation of the file
 *   |- index.latest      - contains the numeric value of the latest generation of the index file (i.e. N from above)
 *   |- incompatible-snapshots - list of all snapshot ids that are no longer compatible with the current version of the cluster
 *   |- snap-20131010.dat - SMILE serialized {@link org.elasticsearch.snapshots.SnapshotInfo} for snapshot "20131010"
 *   |- meta-20131010.dat - SMILE serialized {@link org.elasticsearch.cluster.metadata.Metadata } for snapshot "20131010"
 *   |                      (includes only global metadata)
 *   |- snap-20131011.dat - SMILE serialized {@link org.elasticsearch.snapshots.SnapshotInfo} for snapshot "20131011"
 *   |- meta-20131011.dat - SMILE serialized {@link org.elasticsearch.cluster.metadata.Metadata } for snapshot "20131011"
 *   .....
 *   |- indices/ - data for all indices
 *      |- Ac1342-B_x/ - data for index "foo" which was assigned the unique id Ac1342-B_x (not to be confused with the actual index uuid)
 *      |  |             in the repository
 *      |  |- meta-20131010.dat - JSON Serialized {@link org.elasticsearch.cluster.metadata.IndexMetadata} for index "foo"
 *      |  |- 0/ - data for shard "0" of index "foo"
 *      |  |  |- __1                      \  (files with numeric names were created by older ES versions)
 *      |  |  |- __2                      |
 *      |  |  |- __VPO5oDMVT5y4Akv8T_AO_A |- files from different segments see snap-* for their mappings to real segment files
 *      |  |  |- __1gbJy18wS_2kv1qI7FgKuQ |
 *      |  |  |- __R8JvZAHlSMyMXyZc2SS8Zg /
 *      |  |  .....
 *      |  |  |- snap-20131010.dat - SMILE serialized {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot} for
 *      |  |  |                      snapshot "20131010"
 *      |  |  |- snap-20131011.dat - SMILE serialized {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot} for
 *      |  |  |                      snapshot "20131011"
 *      |  |  |- index-123         - SMILE serialized {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots} for
 *      |  |  |                      the shard. The suffix is the {@link org.elasticsearch.repositories.ShardGeneration } (files with
 *      |  |  |                      numeric suffixes were created by older versions, newer ES versions use a uuid suffix instead)
 *      |  |
 *      |  |- 1/ - data for shard "1" of index "foo"
 *      |  |  |- __1
 *      |  |  |- index-Zc2SS8ZgR8JvZAHlSMyMXy - SMILE serialized {@code BlobStoreIndexShardSnapshots} for the shard
 *      |  |  .....
 *      |  |
 *      |  |-2/
 *      |  ......
 *      |
 *      |- 1xB0D8_B3y/ - data for index "bar" which was assigned the unique id of 1xB0D8_B3y in the repository
 *      ......
 * }
 * </pre>
 *
 * <h2>Getting the Repository's RepositoryData</h2>
 *
 * <p>Loading the {@link org.elasticsearch.repositories.RepositoryData} that holds the list of all snapshots as well as the mapping of
 * indices' names to their repository {@link org.elasticsearch.repositories.IndexId} is done by invoking
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository#getRepositoryData} and implemented as follows:</p>
 * <ol>
 * <li>
 * <ol>
 * <li>The blobstore repository stores the {@code RepositoryData} in blobs named with incrementing suffix {@code N} at {@code /index-N}
 * directly under the repository's root.</li>
 * <li>For each {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} an entry of type
 * {@link org.elasticsearch.cluster.metadata.RepositoryMetadata} exists in the cluster state. It tracks the current valid
 * generation {@code N} as well as the latest generation that a write was attempted for.</li>
 * <li>The blobstore also stores the most recent {@code N} as a 64bit long in the blob {@code /index.latest} directly under the
 * repository's root.</li>
 * </ol>
 * </li>
 * <li>
 * <ol>
 * <li>First, find the most recent {@code RepositoryData} by getting a list of all index-N blobs through listing all blobs with prefix
 * "index-" under the repository root and then selecting the one with the highest value for N.</li>
 * <li>If this operation fails because the repository's {@code BlobContainer} does not support list operations (in the case of read-only
 * repositories), read the highest value of N from the index.latest blob.</li>
 * </ol>
 * </li>
 * <li>
 * <ol>
 * <li>Use the just determined value of {@code N} and get the {@code /index-N} blob and deserialize the {@code RepositoryData} from it.</li>
 * <li>If no value of {@code N} could be found since neither an {@code index.latest} nor any {@code index-N} blobs exist in the repository,
 * it is assumed to be empty and {@link org.elasticsearch.repositories.RepositoryData#EMPTY} is returned.</li>
 * </ol>
 * </li>
 * </ol>
 *
 * <h2>Writing Updated RepositoryData to the Repository</h2>
 *
 * <p>Writing an updated {@link org.elasticsearch.repositories.RepositoryData} to a blob store repository is an operation that uses
 * the cluster state to ensure that a specific {@code index-N} blob is never accidentally overwritten in a master failover scenario.
 * The specific steps to writing a new {@code index-N} blob and thus making changes from a snapshot-create or delete operation visible
 * to read operations on the repository are as follows and all run on the master node:</p>
 *
 * <ol>
 * <li>Write an updated value of {@link org.elasticsearch.cluster.metadata.RepositoryMetadata} for the repository that has the same
 * {@link org.elasticsearch.cluster.metadata.RepositoryMetadata#generation()} as the existing entry and has a value of
 * {@link org.elasticsearch.cluster.metadata.RepositoryMetadata#pendingGeneration()} one greater than the {@code pendingGeneration} of the
 * existing entry.</li>
 * <li>On the same master node, after the cluster state has been updated in the first step, write the new {@code index-N} blob and
 * also update the contents of the {@code index.latest} blob. Note that updating the index.latest blob is done on a best effort
 * basis and that there is a chance for a stuck master-node to overwrite the contents of the {@code index.latest} blob after a newer
 * {@code index-N} has been written by another master node. This is acceptable since the contents of {@code index.latest} are not used
 * during normal operation of the repository and must only be correct for purposes of mounting the contents of a
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} as a read-only url repository.</li>
 * <li>After the write has finished, set the value of {@code RepositoriesState.State#generation} to the value used for
 * {@code RepositoriesState.State#pendingGeneration} so that the new entry for the state of the repository has {@code generation} and
 * {@code pendingGeneration} set to the same value to signalize a clean repository state with no potentially failed writes newer than the
 * last valid {@code index-N} blob in the repository.</li>
 * </ol>
 *
 * <p>If either of the last two steps in the above fails or master fails over to a new node at any point, then a subsequent operation
 * trying to write a new {@code index-N} blob will never use the same value of {@code N} used by a previous attempt. It will always start
 * over at the first of the above three steps, incrementing the {@code pendingGeneration} generation before attempting a write, thus
 * ensuring no overwriting of a {@code index-N} blob ever to occur. The use of the cluster state to track the latest repository generation
 * {@code N} and ensuring no overwriting of {@code index-N} blobs to ever occur allows the blob store repository to properly function even
 * on blob stores with neither a consistent list operation nor an atomic "write but not overwrite" operation.</p>
 *
 * <h2>Creating a Snapshot</h2>
 *
 * <p>Creating a snapshot in the repository happens in the two steps described in detail below.</p>
 *
 * <h3>Writing Shard Data (Segments)</h3>
 *
 * <p>Once all the metadata has been written by the snapshot initialization, the snapshot process moves on to writing the actual shard data
 * to the repository by invoking {@link org.elasticsearch.repositories.Repository#snapshotShard} on the data-nodes that hold the primaries
 * for the shards in the current snapshot. It is implemented as follows:</p>
 *
 * <p>Note:</p>
 * <ul>
 * <li>For each shard {@code i} in a given index, its path in the blob store is located at {@code /indices/${index-snapshot-uuid}/${i}}</li>
 * <li>All the following steps are executed exclusively on the shard's primary's data node.</li>
 * </ul>
 *
 * <ol>
 * <li>Create the {@link org.apache.lucene.index.IndexCommit} for the shard to snapshot.</li>
 * <li>Get the current {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots} blob file with name
 * {@code index-${uuid}} by loading the index shard's generation {@code uuid} from {@link org.elasticsearch.repositories.ShardGenerations}
 * (via {@link org.elasticsearch.repositories.ShardGenerations#getShardGen}). This blob file will list what segment files are already
 * available in the blobstore.</li>
 * <li>By comparing the files in the {@code IndexCommit} and the available file list from the previous step's blob file, determine the new
 * segment files that need to be written to the blob store. For each segment that needs to be added to the blob store, generate a unique
 * name by combining the segment data blob prefix {@code __} and a new UUID and write the segment to the blobstore.</li>
 * <li>After completing all segment writes, a new blob file containing the new shard snapshot's
 * {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot} with name {@code snap-${snapshot-uuid}.dat} is written to
 * the shard's path and contains a list of all the files referenced by the snapshot as well as some metadata about the snapshot. See the
 * documentation of {@code BlobStoreIndexShardSnapshot} for details on its contents.</li>
 * <li>Once all the segments and the {@code BlobStoreIndexShardSnapshot} blob have been written, an updated
 * {@code BlobStoreIndexShardSnapshots} blob is written to the shard's path with name {@code index-${newUUID}}.</li>
 * </ol>
 * At this point, all of the necessary shard data and shard metadata for the new shard snapshot have been written to the repository, but the
 * metadata outside of the shard directory has not been updated to point to the new shard snapshot as the latest. The next finalization step
 * will handle updates external to the index shard directory, and add references in the root directory.
 *
 * <h3>Finalizing the Snapshot</h3>
 *
 * <p>After all primaries have finished writing the necessary segment files to the blob store in the previous step, the master node moves on
 * to finalizing the snapshot by invoking {@link org.elasticsearch.repositories.Repository#finalizeSnapshot}. This method executes the
 * following actions in order:</p>
 * <ol>
 * <li>Write a blob containing the cluster metadata to the root of the blob store repository at {@code /meta-${snapshot-uuid}.dat}</li>
 * <li>Write the metadata for the index to a blob in that index's directory at
 * {@code /indices/${index-snapshot-uuid}/meta-${snapshot-uuid}.dat}</li>
 * <li>Write the {@link org.elasticsearch.snapshots.SnapshotInfo} blob for the given snapshot in a new blob file
 * {@code /snap-${snapshot-uuid}.dat} directly under the repository root.</li>
 * <li>Write an updated {@code RepositoryData} blob containing the new snapshot.</li>
 * </ol>
 *
 * <h2>Deleting a Snapshot</h2>
 *
 * <p>Deleting a snapshot is an operation that is exclusively executed on the master node that runs through the following sequence of
 * action when {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository#deleteSnapshots} is invoked:</p>
 *
 * <ol>
 * <li>Get the current {@code RepositoryData} from the latest {@code index-N} blob at the repository root.</li>
 * <li>For each index referenced by the snapshot:
 * <ol>
 * <li>Delete the snapshot's {@code IndexMetadata} at {@code /indices/${index-snapshot-uuid}/meta-${snapshot-uuid}}.</li>
 * <li>Go through all shard directories {@code /indices/${index-snapshot-uuid}/${i}} and:
 * <ol>
 * <li>Remove the {@code BlobStoreIndexShardSnapshot} blob at {@code /indices/${index-snapshot-uuid}/${i}/snap-${snapshot-uuid}.dat}.</li>
 * <li>List all blobs in the shard path {@code /indices/${index-snapshot-uuid}} and build a new {@code BlobStoreIndexShardSnapshots} from
 * the remaining {@code BlobStoreIndexShardSnapshot} blobs in the shard. Afterwards, write it to the next shard generation blob at
 * {@code /indices/${index-snapshot-uuid}/${i}/index-${uuid}} (The shard's generation is determined from the map of shard generations in
 * the {@link org.elasticsearch.repositories.RepositoryData} in the root {@code index-${N}} blob of the repository.</li>
 * <li>Collect all segment blobs (identified by having the data blob prefix {@code __}) in the shard directory which are not referenced by
 * the new {@code BlobStoreIndexShardSnapshots} that has been written in the previous step as well as the previous index-${uuid}
 * blob so that it can be deleted at the end of the snapshot delete process.</li>
 * </ol>
 * </li>
 * <li>Write an updated {@code RepositoryData} blob with the deleted snapshot removed and containing the updated repository generations
 * that changed for the shards affected by the delete.</li>
 * <li>Delete the global {@code Metadata} blob {@code meta-${snapshot-uuid}.dat} stored directly under the repository root for the snapshot
 * as well as the {@code SnapshotInfo} blob at {@code /snap-${snapshot-uuid}.dat}.</li>
 * <li>Delete all unreferenced blobs previously collected when updating the shard directories. Also, remove any index folders or blobs
 * under the repository root that are not referenced by the new {@code RepositoryData} written in the previous step.</li>
 * </ol>
 * </li>
 * </ol>
 */
package org.elasticsearch.repositories.blobstore;
