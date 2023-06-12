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

/**
 * <p>This package exposes stateless masters functionality</p>
 *
 * <h2>Summary</h2>
 * Stateless masters rely on an external blob store to elect a leader node and
 * to store the {@link org.elasticsearch.cluster.ClusterState} safely.
 *
 * <p>All stateless master files are located under the {@code cluster_state/} key prefix within the blob store.</p>
 *
 * <p>The next listing is an example of the blob store contents of an stateless cluster state files</p>
 * <pre>
 *     cluster_state/lease
 *     cluster_state/1/_0.cfe
 *     cluster_state/1/_0.cfs
 *     cluster_state/1/_0.si
 *     cluster_state/1/segments_a
 *     cluster_state/2/_1.cfe
 *     cluster_state/2/_1.cfs
 *     cluster_state/2/_1.si
 *     cluster_state/2/segments_b
 * </pre>
 *
 * <h2>Leader election strategy</h2>
 *
 * In stateless there's still a unique leader per term that process all cluster state updates. The leader node is still
 * elected among the master-eligible nodes but using a different approach than in stateful Elasticsearch. Instead of requiring
 * a quorum of nodes voting for a node to become the new leader, stateless uses the blob store to claim the ownership
 * of a new term in a lease stored in {@code cluster_state/lease}. This lease is claimed once per term using a CAS
 * operation {@link org.elasticsearch.common.blobstore.BlobContainer#compareAndSetRegister}.
 * Once a new term is granted to a node, it's guaranteed that only that node will be responsible for the cluster
 * state updates in that term.
 * <p>This strategy is implemented in {@link co.elastic.elasticsearch.stateless.cluster.coordination.StatelessElectionStrategy}.</p>
 *
 * <h2>Cluster state storage</h2>
 *
 * <p>Once a leader is elected, cluster state processing works as usual, but instead of requiring a quorum of nodes acknowledging
 * every cluster state update, the new cluster state is stored safely into the blob store by the elected leader node.
 * Cluster state lucene files are namespaced under the term they belong to in the blob store, i.e. all the cluster state files
 * that belong to term 1 are stored under the prefix {@code cluster_state/1/<lucene_file>}.</p>
 *
 * <p>When the leader node persists a new cluster state update, it's written locally
 * and then uploaded to the blob store in a blocking fashion.</p>
 * <p>{@link co.elastic.elasticsearch.stateless.cluster.coordination.BlobStoreSyncDirectory} is an specialized Lucene Directory
 * that intercepts the fsync calls and uploads the new files to the blob store instead of fsyncing them in order to ensure their
 * durability.</p>
 * <p>Additionally, this Directory takes care of cleaning unnecessary files once a new Lucene commit
 * is safely stored into the blob store.</p>
 */
package co.elastic.elasticsearch.stateless.cluster.coordination;
